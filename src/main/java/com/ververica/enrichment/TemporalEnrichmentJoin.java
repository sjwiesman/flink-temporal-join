package com.ververica.enrichment;

import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;

/**
 * Similar to a FlinkSQL <a
 * href="https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/streaming/joins.html#join-with-a-temporal-table">Temporal
 * Table Join</a> with the following differences:
 *
 * <ul>
 *   <li>Support for left-outer joins
 *   <li>Parameterized allowed lateness
 * </ul>
 *
 * @param <IN1> Type of the input
 * @param <IN2> Type of the join side
 * @param <OUT> Type of the output
 */
public class TemporalEnrichmentJoin<IN1, IN2, OUT> extends CoProcessFunction<IN1, IN2, OUT> {

    private final JoinFunction<IN1, IN2, OUT> function;

    private final TypeInformation<IN2> typeInfo;

    private final long ttl;

    private transient MapState<Long, IN2> state;

    /**
     * @param function The join function called for every element on input side 1.
     * @param typeInfo The {@link TypeInformation} for the probe side of the join.
     * @param ttl The amount of the time a value may sit in the probe side of a join before being
     *     clean up.
     */
    public TemporalEnrichmentJoin(JoinFunction<IN1, IN2, OUT> function, TypeInformation<IN2> typeInfo, Time ttl) {
        this.function = function;
        this.typeInfo = typeInfo;

        this.ttl = ttl.toMilliseconds();
    }

    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<Long, IN2> descriptor = new MapStateDescriptor<>("join-state", Types.LONG, typeInfo);
        state = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void processElement1(IN1 value, Context ctx, Collector<OUT> out) throws Exception {

        IN2 other = null;
        long search = Long.MIN_VALUE;

        for (Long timestamp : state.keys()) {
            if (timestamp <= ctx.timestamp() && timestamp > search) {
                search = timestamp;
            }
        }

        if (search != Long.MIN_VALUE) {
            other = state.get(search);
        }

        OUT result = function.join(value, other);

        if (result != null) {
            out.collect(result);
        }
    }

    @Override
    public void processElement2(IN2 value, Context ctx, Collector<OUT> out) throws Exception {
        long cleanUp = ctx.timestamp() + ttl;

        ctx.timerService().registerEventTimeTimer(cleanUp);

        state.put(ctx.timestamp(), value);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUT> out) throws Exception {
        long start = timestamp - ttl;

        state.remove(start);
    }
}
