package com.ververica;

import com.ververica.enrichment.TemporalEnrichmentJoin;
import com.ververica.join.CurrencyConversion;
import com.ververica.models.CurrencyRate;
import com.ververica.models.NormalizedTransaction;
import com.ververica.models.Transaction;
import com.ververica.sources.BaseGenerator;
import com.ververica.watermark.CurrencyRateTimestamper;
import com.ververica.watermark.TransactionTimestamper;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;

public class StreamingJob {

	private static List<Integer> currency;

	static {
		currency = new ArrayList<>(3);
		currency.add(1);
		currency.add(2);
		currency.add(3);
	}

	public static void main(String[] args) throws Exception {
        ParameterTool tool = ParameterTool.fromArgs(args);

		int currencyRate    = tool.getInt("currency", 100);
		int transactionRate = tool.getInt("transactions", 10_000);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.getConfig().enableObjectReuse();
		env.getConfig().disableGenericTypes();

		env.enableCheckpointing(10 * 1000);

		DataStream<CurrencyRate> rates = env
				.addSource(new CurrencyRateSource(currencyRate))
				.name("rates")
				.assignTimestampsAndWatermarks(new CurrencyRateTimestamper())
				.name("timestamp-assigner");

		DataStream<Transaction> transactions = env
				.addSource(new TransactionSource(transactionRate))
				.name("transactions")
				.assignTimestampsAndWatermarks(new TransactionTimestamper())
				.name("timestamp-assigner");

		transactions.connect(rates)
				.keyBy(Transaction::getCurrency, CurrencyRate::getId)
				.process(new TemporalEnrichmentJoin<>(
						new CurrencyConversion(),
						TypeInformation.of(CurrencyRate.class),
						Time.days(7)))
				.returns(NormalizedTransaction.class)
				.name("currency-conversion")
                .disableChaining()
                .addSink(new DiscardingSink<>())
                .name("normalized-transaction-sink");

		env.execute("Currency Conversion Job");
	}

	static class CurrencyRateSource extends BaseGenerator<CurrencyRate> {
		private int i;

		CurrencyRateSource(int maxRecordsPerSecond) {
			super(maxRecordsPerSecond);

			i = 0;
		}

		@Override
		protected CurrencyRate randomEvent(SplittableRandom rnd, long id) {
			int index = i;
			i = ++i % currency.size();

			return new CurrencyRate(currency.get(index), System.currentTimeMillis(), rnd.nextDouble(0.1, 1.0));
		}
	}

	static class TransactionSource extends BaseGenerator<Transaction> {
		private int i;

		TransactionSource(int maxRecordsPerSecond) {
			super(maxRecordsPerSecond);
			i = 0;
		}

		@Override
		protected Transaction randomEvent(SplittableRandom rnd, long id) {

			return new Transaction(
					id,
					System.currentTimeMillis(),
					currency.get(i),
					rnd.nextDouble(0.1, 100.0)
			);
		}
	}
}
