package com.ververica.models;

import java.util.Objects;

public final class CurrencyRate {
    private long id;
    private long timestamp;
    private double rate;

    public CurrencyRate() {}

    public CurrencyRate(long id, long timestamp, double rate) {
        this.id = id;
        this.timestamp = timestamp;
        this.rate = rate;
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

    public double getRate() {
        return rate;
    }

    public void setRate(double rate) {
        this.rate = rate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CurrencyRate that = (CurrencyRate) o;
        return id == that.id &&
                timestamp == that.timestamp &&
                Double.compare(that.rate, rate) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, timestamp, rate);
    }

    @Override
    public String toString() {
        return "CurrencyRate{" +
                "id=" + id +
                ", timestamp=" + timestamp +
                ", rate=" + rate +
                '}';
    }
}
