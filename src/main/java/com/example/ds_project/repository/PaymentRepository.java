package com.example.ds_project.repository;

import com.example.ds_project.model.Payment;
import org.springframework.stereotype.Repository;

import java.util.*;

@Repository
public class PaymentRepository {

    private Map<String, Payment> paymentStore = new java.util.concurrent.ConcurrentHashMap<>();

    public Payment save(Payment payment) {
        paymentStore.put(payment.getId(), payment);
        return payment;
    }

    public Optional<Payment> findById(String id) {
        return Optional.ofNullable(paymentStore.get(id));
    }

    public List<Payment> findAll() {
        return new ArrayList<>(paymentStore.values());
    }

    public long count() {
        return paymentStore.size();
    }
}