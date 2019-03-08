package com.ververica.models;

import java.util.Objects;

public class NormalizedTransaction {
    private long id;
    private long timestamp;
    private double amount;

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
        NormalizedTransaction that = (NormalizedTransaction) o;
        return id == that.id &&
                timestamp == that.timestamp &&
                Double.compare(that.amount, amount) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, timestamp, amount);
    }

    @Override
    public String toString() {
        return "NormalizedTransaction{" +
                "id=" + id +
                ", timestamp=" + timestamp +
                ", amount=" + amount +
                '}';
    }
}
