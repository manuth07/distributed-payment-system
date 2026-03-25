package com.example.ds_project.controller;

import com.example.ds_project.coordination.LeaderState;
import com.example.ds_project.model.Payment;
import com.example.ds_project.service.PaymentService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.util.List;

@RestController
@RequestMapping("/payments")
public class PaymentController {
    private final PaymentService service;
    private final LeaderState leaderState;
    private final RestTemplate restTemplate;

    public PaymentController(PaymentService service, LeaderState leaderState, RestTemplate restTemplate) {
        this.service = service;
        this.leaderState = leaderState;
        this.restTemplate = restTemplate;
    }

    @PostMapping
    public Payment makePayment(@RequestParam BigDecimal amount) {
        if (leaderState.isLeader()) {
            return service.processPayment(amount);
        } else {
            return forwardToLeader(amount, 0);
        }
    }

    private Payment forwardToLeader(BigDecimal amount, int retryCount) {
        String leaderUrl = leaderState.getLeaderUrl();
        if (leaderUrl == null) {
            throw new IllegalStateException("Leader not yet elected or unknown.");
        }
        String url = leaderUrl + "/payments?amount=" + amount;
        try {
            ResponseEntity<Payment> response = restTemplate.postForEntity(url, null, Payment.class);
            return response.getBody();
        } catch (Exception e) {
            if (retryCount < 6) {
                System.out.println("Forwarding to leader failed. Retrying in 1s (Attempt " + (retryCount + 1) + "/6)...");
                try {
                    Thread.sleep(1000); 
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                return forwardToLeader(amount, retryCount + 1);
            } else {
                throw new RuntimeException("Failed to forward payment to leader after retries", e);
            }
        }
    }

    @GetMapping
    public List<Payment> getAllPayments() {
        return service.getAllPayments();
    }
}
