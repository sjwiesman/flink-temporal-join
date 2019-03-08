package com.ververica.join;

import javax.annotation.Nullable;

import com.ververica.models.CurrencyRate;
import com.ververica.models.NormalizedTransaction;
import com.ververica.models.Transaction;

import org.apache.flink.api.common.functions.JoinFunction;

public class CurrencyConversion implements JoinFunction<Transaction, CurrencyRate, NormalizedTransaction> {
    @Override
    public NormalizedTransaction join(Transaction first, @Nullable CurrencyRate second) {
        NormalizedTransaction transaction = new NormalizedTransaction();
        transaction.setId(first.getId());
        transaction.setTimestamp(first.getTimestamp());

        if (second != null) {
            double normalized = second.getRate() * first.getAmount();
            transaction.setAmount(normalized);
        } else {
            transaction.setAmount(first.getAmount());
        }

        return transaction;
    }
}
