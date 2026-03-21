package com.example.ds_project.controller;

import com.example.ds_project.model.Payment;
import com.example.ds_project.repository.PaymentRepository;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/internal")
public class InternalController {

    private final PaymentRepository repository;

    public InternalController(PaymentRepository repository) {
        this.repository = repository;
    }

    @PostMapping("/replicate")
    public void replicatePayment(@RequestBody Payment payment) {

        // Deduplication check
        if (repository.findById(payment.getId()).isPresent()) {
            return;
        }

        repository.save(payment);
    }

    //Fault tolerance base
    @GetMapping("/health")
    public String health() {
        return "OK";
    }
}