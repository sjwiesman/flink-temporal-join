package com.ververica.models;

import java.util.Objects;

public final class Transaction {
    private long id;
    private long timestamp;
    private long currency;
    private double amount;


    public Transaction() {}

    public Transaction(long id, long timestamp, long currency, double amount) {
        this.id = id;
        this.timestamp = timestamp;
        this.currency = currency;
        this.amount = amount;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getCurrency() {
        return currency;
    }

    public void setCurrency(long currency) {
        this.currency = currency;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Transaction that = (Transaction) o;
        return id == that.id &&
                timestamp == that.timestamp &&
                currency == that.currency &&
                Double.compare(that.amount, amount) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, timestamp, currency, amount);
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "id=" + id +
                ", timestamp=" + timestamp +
                ", currency=" + currency +
                ", amount=" + amount +
                '}';
    }
}
