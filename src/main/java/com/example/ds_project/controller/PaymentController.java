package com.example.ds_project.controller;

import com.example.ds_project.model.Payment;
import com.example.ds_project.service.PaymentService;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.List;

@RestController
@RequestMapping("/payments")
public class PaymentController {
    private final PaymentService service;

    public PaymentController(PaymentService service) {
        this.service = service;
    }
    @PostMapping
    public Payment makePayment(@RequestParam BigDecimal amount) {
        return service.processPayment(amount);
    }
    @GetMapping
    public List<Payment> getAllPayments() {
        return service.getAllPayments();
    }
}
