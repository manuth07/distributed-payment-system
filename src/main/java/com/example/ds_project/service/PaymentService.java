package com.example.ds_project.service;

import com.example.ds_project.config.NodeConfig;
import com.example.ds_project.model.Payment;
import com.example.ds_project.repository.PaymentRepository;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

@Service
public class PaymentService {
    private final PaymentRepository repository;
    private final NodeConfig nodeConfig;
    private final ReplicationService replicationService;


    public PaymentService(PaymentRepository repository, NodeConfig nodeConfig, ReplicationService replicationService) {
        this.repository = repository;
        this.nodeConfig = nodeConfig;
        this.replicationService = replicationService;
    }
    public Payment processPayment(BigDecimal amount) {
        Payment newPayment = new Payment(amount, nodeConfig.getNodeId());
        Payment saved = repository.save(newPayment);

        // Trigger replication
        replicationService.replicateToOtherNodes(saved);

        return saved;
    }
    public List<Payment> getAllPayments(){
        return repository.findAll();
    }
}
