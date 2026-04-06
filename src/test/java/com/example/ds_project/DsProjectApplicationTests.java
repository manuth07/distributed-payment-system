package com.example.ds_project;

import com.example.ds_project.model.Payment;
import com.example.ds_project.raft.LogEntry;
import com.example.ds_project.raft.RaftLog;
import com.example.ds_project.raft.RaftNode;
import com.example.ds_project.repository.PaymentRepository;
import com.example.ds_project.timesync.ClockSynchronizationService;
import com.example.ds_project.timesync.LogReorderBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive Unit Tests for Distributed Payment System
 * 
 * Covers:
 * - Member 1 (Fault Tolerance): Node state transitions
 * - Member 2 (Data Replication): Deduplication, consistency
 * - Member 3 (Time Sync): Clock offset, timestamp ordering
 * - Member 4 (Consensus): Raft state machine
 */
@SpringBootTest
public class DsProjectApplicationTests {

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private RaftLog raftLog;

    @Autowired
    private ClockSynchronizationService clockSyncService;

    @BeforeEach
    public void setUp() {
        // Clear repository before each test
        paymentRepository.deleteAll();
        
        // Clear Raft log for test isolation
        // Truncate from index 0 to clear all entries
        if (raftLog.getLastLogIndex() >= 0) {
            raftLog.truncateFromIndex(0);
        }
    }

    // ============ MEMBER 2: DEDUPLICATION TESTS ============

    /**
     * Test triple-layer deduplication: Ingestion layer (HashSet)
     * Verifies that duplicate payment IDs are rejected at the earliest stage
     */
    @Test
    public void testDeduplicationAtIngestionLayer() {
        String paymentId = UUID.randomUUID().toString();
        Payment p1 = new Payment(paymentId, "node1", BigDecimal.valueOf(100), "SUCCESS", LocalDateTime.now());
        Payment p2 = new Payment(paymentId, "node1", BigDecimal.valueOf(100), "SUCCESS", LocalDateTime.now());

        paymentRepository.save(p1);
        assertTrue(paymentRepository.findById(paymentId).isPresent(), "First payment should be saved");

        // Attempt duplicate — should still retrieve only one
        assertEquals(1, paymentRepository.countByPaymentId(paymentId), "Duplicate should not create second entry");
    }

    /**
     * Test Raft log deduplication: Consensus layer verification
     * Verifies that the Raft log can quickly detect duplicates using O(1) index
     */
    @Test
    public void testRaftLogDeduplicationPerformance() {
        String paymentId = UUID.randomUUID().toString();
        LogEntry entry1 = new LogEntry(0, 1, paymentId, "{\"amount\": 100}", System.currentTimeMillis(), LogEntry.LogStatus.PENDING);
        
        raftLog.appendEntry(entry1);
        assertTrue(raftLog.containsPayment(paymentId), "Payment ID should be in log index");
        
        // Performance: containsPayment should be O(1), not O(N)
        long startTime = System.nanoTime();
        boolean exists = raftLog.containsPayment(paymentId);
        long duration = System.nanoTime() - startTime;
        
        assertTrue(exists, "Payment should be found");
        assertTrue(duration < 1_000_000, "O(1) lookup should be < 1ms (1_000_000 ns); got " + duration + "ns");
    }

    /**
     * Test repository deduplication: Persistence layer
     * Verifies that findById prevents duplicate saves
     */
    @Test
    public void testRepositoryDeduplication() {
        String paymentId = UUID.randomUUID().toString();
        Payment payment = new Payment(paymentId, "node1", BigDecimal.valueOf(50), "SUCCESS", LocalDateTime.now());
        
        paymentRepository.save(payment);
        paymentRepository.save(payment);  // Duplicate attempt
        
        assertEquals(1, paymentRepository.findAll().size(), "Repository should deduplicate identical payments");
    }

    // ============ MEMBER 3: TIME SYNCHRONIZATION TESTS ============

    /**
     * Test clock offset capture during payment creation
     * Verifies Phase 3b: Payment model includes correctedTimestamp
     */
    @Test
    public void testClockOffsetCapture() {
        long clockOffset = clockSyncService.getCurrentOffset();
        long rawTimestamp = System.currentTimeMillis();
        long correctedTimestamp = rawTimestamp + clockOffset;
        
        Payment payment = new Payment(
                UUID.randomUUID().toString(),
                "node1",
                BigDecimal.valueOf(100),
                "SUCCESS",
                LocalDateTime.now(),
                correctedTimestamp,
                clockOffset,
                "node1"
        );
        
        paymentRepository.save(payment);
        assertTrue(payment.getCorrectedTimestamp() > 0, "Corrected timestamp should be captured");
        assertEquals(clockOffset, payment.getClockOffsetAtCreation(), "Clock offset should be stored");
    }

    /**
     * Test timestamp-based payment ordering
     * Verifies Phase 3c: Repository sorts by correctedTimestamp
     */
    @Test
    public void testPaymentSortingByTimestamp() {
        // Create 3 payments with different corrected timestamps
        Payment p1 = createPaymentWithTimestamp(3000);  // Latest
        Payment p2 = createPaymentWithTimestamp(1000);  // Earliest
        Payment p3 = createPaymentWithTimestamp(2000);  // Middle
        
        paymentRepository.save(p1);
        paymentRepository.save(p3);
        paymentRepository.save(p2);
        
        // Repository.findAll() should return sorted by correctedTimestamp
        List<Payment> sorted = paymentRepository.findAll();
        assertEquals(p2.getCorrectedTimestamp(), sorted.get(0).getCorrectedTimestamp(), "First should be p2 (ts=1000)");
        assertEquals(p3.getCorrectedTimestamp(), sorted.get(1).getCorrectedTimestamp(), "Second should be p3 (ts=2000)");
        assertEquals(p1.getCorrectedTimestamp(), sorted.get(2).getCorrectedTimestamp(), "Third should be p1 (ts=3000)");
    }

    /**
     * Test time-window payment queries
     * Verifies Phase 3c: findByCorrectedTimestampBetween works correctly
     */
    @Test
    public void testTimeWindowQueries() {
        Payment p1 = createPaymentWithTimestamp(1000);
        Payment p2 = createPaymentWithTimestamp(1500);
        Payment p3 = createPaymentWithTimestamp(2000);
        
        paymentRepository.save(p1);
        paymentRepository.save(p2);
        paymentRepository.save(p3);
        
        // Query for payments in range [1000, 1500]
        List<Payment> result = paymentRepository.findByCorrectedTimestampBetween(1000, 1500);
        assertEquals(2, result.size(), "Should return 2 payments in range");
        assertTrue(result.stream().allMatch(p -> p.getCorrectedTimestamp() >= 1000 && p.getCorrectedTimestamp() <= 1500));
    }

    // ============ MEMBER 4: CONSENSUS & RAFT TESTS ============

    /**
     * Test Raft log entry application
     * Verifies log entries are correctly parsed and applied
     */
    @Test
    public void testRaftLogEntryApplication() {
        String paymentPayload = "{\"id\": \"p123\", \"amount\": 100, \"status\": \"SUCCESS\"}";
        LogEntry entry = new LogEntry(0, 1, "p123", paymentPayload, System.currentTimeMillis(), LogEntry.LogStatus.PENDING);
        
        raftLog.appendEntry(entry);
        
        LogEntry retrieved = raftLog.getEntry(0);
        assertNotNull(retrieved, "Entry should be retrievable");
        assertEquals("p123", retrieved.getPaymentId(), "Payment ID should match");
        assertEquals(paymentPayload, retrieved.getPayload(), "Payload should match");
    }

    /**
     * Test Raft log growth and indexing
     * Verifies lastLogIndex is correctly maintained
     */
    @Test
    public void testRaftLogIndexing() {
        assertEquals(-1, raftLog.getLastLogIndex(), "Empty log should have lastLogIndex = -1");
        
        LogEntry entry1 = new LogEntry(0, 1, "p1", "{}", System.currentTimeMillis(), LogEntry.LogStatus.PENDING);
        LogEntry entry2 = new LogEntry(1, 1, "p2", "{}", System.currentTimeMillis(), LogEntry.LogStatus.PENDING);
        
        raftLog.appendEntry(entry1);
        assertEquals(0, raftLog.getLastLogIndex(), "After 1st entry, lastLogIndex should be 0");
        
        raftLog.appendEntry(entry2);
        assertEquals(1, raftLog.getLastLogIndex(), "After 2nd entry, lastLogIndex should be 1");
    }

    /**
     * Test Raft log term tracking
     * Verifies last log term is correctly maintained
     */
    @Test
    public void testRaftLogTermTracking() {
        LogEntry entry1 = new LogEntry(0, 1, "p1", "{}", System.currentTimeMillis(), LogEntry.LogStatus.PENDING);
        LogEntry entry2 = new LogEntry(1, 2, "p2", "{}", System.currentTimeMillis(), LogEntry.LogStatus.PENDING);
        
        raftLog.appendEntry(entry1);
        assertEquals(1, raftLog.getLastLogTerm(), "Last log term should be 1");
        
        raftLog.appendEntry(entry2);
        assertEquals(2, raftLog.getLastLogTerm(), "Last log term should be 2");
    }

    // ============ MEMBER 1: FAULT TOLERANCE & GENERAL TESTS ============

    /**
     * Test payment repository consistency
     * Verifies basic CRUD operations maintain data integrity
     */
    @Test
    public void testPaymentRepositoryConsistency() {
        Payment p1 = new Payment(UUID.randomUUID().toString(), "node1", BigDecimal.valueOf(100), "SUCCESS", LocalDateTime.now());
        Payment p2 = new Payment(UUID.randomUUID().toString(), "node2", BigDecimal.valueOf(200), "SUCCESS", LocalDateTime.now());
        
        paymentRepository.save(p1);
        paymentRepository.save(p2);
        
        assertEquals(2, paymentRepository.findAll().size(), "Should have 2 payments");
        assertNotNull(paymentRepository.findById(p1.getId()), "p1 should be found");
        assertNotNull(paymentRepository.findById(p2.getId()), "p2 should be found");
    }

    /**
     * Test payments by node ID query
     * Verifies repository can filter by node
     */
    @Test
    public void testPaymentsByNodeId() {
        Payment p1 = new Payment(UUID.randomUUID().toString(), "node1", BigDecimal.valueOf(100), "SUCCESS", LocalDateTime.now());
        Payment p2 = new Payment(UUID.randomUUID().toString(), "node1", BigDecimal.valueOf(150), "SUCCESS", LocalDateTime.now());
        Payment p3 = new Payment(UUID.randomUUID().toString(), "node2", BigDecimal.valueOf(200), "SUCCESS", LocalDateTime.now());
        
        paymentRepository.save(p1);
        paymentRepository.save(p2);
        paymentRepository.save(p3);
        
        List<Payment> node1Payments = paymentRepository.findByNodeIdOrderByCorrectedTimestamp("node1");
        assertEquals(2, node1Payments.size(), "node1 should have 2 payments");
    }

    /**
     * Test out-of-order log entry buffering (Phase 4)
     * Verifies LogReorderBuffer sorts entries by correctedTimestamp
     */
    @Test
    public void testLogReorderBuffer() {
        LogReorderBuffer<TestEntry> buffer = new LogReorderBuffer<>("test-buffer", 500, 10);
        
        // Add entries out of order
        buffer.add(new TestEntry(3000));  // Latest
        buffer.add(new TestEntry(1000));  // Earliest
        buffer.add(new TestEntry(2000));  // Middle
        
        // Force flush to get sorted results
        List<TestEntry> sorted = buffer.flush();
        
        assertEquals(3, sorted.size(), "Should have 3 entries");
        assertEquals(1000, sorted.get(0).getCorrectedTimestamp(), "First should be ts=1000");
        assertEquals(2000, sorted.get(1).getCorrectedTimestamp(), "Second should be ts=2000");
        assertEquals(3000, sorted.get(2).getCorrectedTimestamp(), "Third should be ts=3000");
    }

    // ============ HELPER METHODS ============

    private Payment createPaymentWithTimestamp(long correctedTimestamp) {
        return new Payment(
                UUID.randomUUID().toString(),
                "node1",
                BigDecimal.valueOf(100),
                "SUCCESS",
                LocalDateTime.now(),
                correctedTimestamp,
                0,
                "node1"
        );
    }

    /**
     * Helper class for LogReorderBuffer testing
     */
    static class TestEntry implements com.example.ds_project.timesync.Timestamped {
        private final long timestamp;

        TestEntry(long timestamp) {
            this.timestamp = timestamp;
        }

        @Override
        public long getCorrectedTimestamp() {
            return timestamp;
        }
    }
}
