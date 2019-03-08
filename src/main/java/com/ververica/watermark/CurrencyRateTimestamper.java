package com.ververica.watermark;

import com.ververica.models.CurrencyRate;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class CurrencyRateTimestamper extends BoundedOutOfOrdernessTimestampExtractor<CurrencyRate> {

    public CurrencyRateTimestamper() {
        super(Time.seconds(1));
    }

    @Override
    public long extractTimestamp(CurrencyRate currencyRate) {
        return currencyRate.getTimestamp();
    }
}
