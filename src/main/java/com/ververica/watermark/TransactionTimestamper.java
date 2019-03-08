package com.ververica.watermark;

import com.ververica.models.Transaction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TransactionTimestamper extends BoundedOutOfOrdernessTimestampExtractor<Transaction> {

    public TransactionTimestamper() {
        super(Time.seconds(3));
    }

    @Override
    public long extractTimestamp(Transaction transaction) {
        return transaction.getTimestamp();
    }
}
