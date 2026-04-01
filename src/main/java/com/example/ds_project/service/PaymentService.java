package com.example.ds_project.service;

import com.example.ds_project.kafka.PaymentEvent;
import com.example.ds_project.model.Payment;
import com.example.ds_project.repository.PaymentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

@Service
public class PaymentService {
    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository repository;

    @Value("${server.port}")
    private String serverPort;

    public PaymentService(PaymentRepository repository) {
        this.repository = repository;
    }

    public void processPayment(PaymentEvent event) {
        log.info("Processing payment {}", event.paymentId());

        try {
            // Task 2: Simulate processing
            Thread.sleep(75);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Create Payment and set status
        Payment payment = new Payment(
                event.paymentId().toString(),
                "node-" + serverPort,
                event.amount(),
                "SUCCESS",
                LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(event.timestamp()), ZoneOffset.UTC)
        );

        // Task 3: ALWAYS save after processing
        repository.save(payment);

        log.info("Payment {} processed successfully", event.paymentId());
        log.info("Payment {} saved in node {}", event.paymentId(), serverPort);
    }

    public List<Payment> getAllPayments() {
        return repository.findAll();
    }

    public long getCount() {
        return repository.count();
    }
}
