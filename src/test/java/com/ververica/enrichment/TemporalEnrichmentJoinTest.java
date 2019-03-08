package com.ververica.enrichment;

import com.ververica.join.CurrencyConversion;
import com.ververica.models.CurrencyRate;
import com.ververica.models.NormalizedTransaction;
import com.ververica.models.Transaction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TemporalEnrichmentJoinTest {
    private static final Time TTL = Time.milliseconds(100);

    @Test
    public void testValuesAreInsertedAndThenDeleted() throws Exception {
        try (KeyedTwoInputStreamOperatorTestHarness<Long, Transaction, CurrencyRate, NormalizedTransaction> testHarness = create()) {

            testHarness.setup();
            testHarness.open();

            testHarness.processElement2(new StreamRecord<>(new CurrencyRate(1, 0, 0.5), 0));
            testHarness.processElement2(new StreamRecord<>(new CurrencyRate(1, 10, 0.5), 10));

            // because the values are stored in a map state TestHarness#numKeyedEntries always returns 1
            // we have to check the number of elements by way of timers instead
            Assert.assertEquals(
                    "Incorrect number of entries, each value should register its own cleanup timer",
                    2,
                    testHarness.numEventTimeTimers());

            Watermark watermark = new Watermark(9 + TTL.toMilliseconds());

            testHarness.processWatermark1(watermark);
            testHarness.processWatermark2(watermark);

            Assert.assertEquals("Timers should only clean up expired values", 1, testHarness.numEventTimeTimers());
        }
    }

    @Test
    public void testTemporalJoin() throws Exception {
        try (KeyedTwoInputStreamOperatorTestHarness<Long, Transaction, CurrencyRate, NormalizedTransaction> testHarness = create()) {
            testHarness.setup();
            testHarness.open();

            testHarness.processElement2(new StreamRecord<>(new CurrencyRate(1, 0, 0.5), 0));
            testHarness.processElement1(new StreamRecord<>(new Transaction(10, 10, 1, 2), 10));

            List<StreamRecord<? extends NormalizedTransaction>> output =
                    testHarness.extractOutputStreamRecords();
            Assert.assertEquals("Incorrect number of output", 1, output.size());
            Assert.assertEquals(
                    "Incorrect currency conversion", 1.0, output.get(0).getValue().getAmount(), 0.0001);
        }
    }

    @Test
    public void testTemporalJoinChoosesCorrectWindow() throws Exception {
        try (KeyedTwoInputStreamOperatorTestHarness<Long, Transaction, CurrencyRate, NormalizedTransaction> testHarness = create()) {
            testHarness.setup();
            testHarness.open();

            testHarness.processElement2(new StreamRecord<>(new CurrencyRate(1, 0, 0.5), 0));
            testHarness.processElement2(new StreamRecord<>(new CurrencyRate(1, 9, 2.0), 9));

            testHarness.processElement1(new StreamRecord<>(new Transaction(10, 10, 1, 2), 10));

            List<StreamRecord<? extends NormalizedTransaction>> output = testHarness.extractOutputStreamRecords();
            Assert.assertEquals("Incorrect number of output", 1, output.size());
            Assert.assertEquals("Incorrect currency conversion", 4.0, output.get(0).getValue().getAmount(), 0.0001);
        }
    }

    private KeyedTwoInputStreamOperatorTestHarness<
            Long, Transaction, CurrencyRate, NormalizedTransaction>
    create() throws Exception {
        CurrencyConversion conversion = new CurrencyConversion();

        TemporalEnrichmentJoin<Transaction, CurrencyRate, NormalizedTransaction> join = new TemporalEnrichmentJoin<>(
                        conversion,
                        TypeInformation.of(CurrencyRate.class),
                        TemporalEnrichmentJoinTest.TTL);

        return new KeyedTwoInputStreamOperatorTestHarness<>(
                new KeyedCoProcessOperator<>(join),
                Transaction::getCurrency,
                CurrencyRate::getId,
                Types.LONG);
    }
}
