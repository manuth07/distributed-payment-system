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

    public PaymentService(PaymentRepository repository, NodeConfig nodeConfig) {
        this.repository = repository;
        this.nodeConfig = nodeConfig;
    }
    public Payment processPayment(BigDecimal amount) {
        Payment newPayment = new Payment(amount, nodeConfig.getNodeId());
        return repository.save(newPayment);
    }
    public List<Payment> getAllPayments(){
        return repository.findAll();
    }
}
