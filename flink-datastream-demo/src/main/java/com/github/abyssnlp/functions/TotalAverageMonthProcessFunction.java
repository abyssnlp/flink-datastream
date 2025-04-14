package com.github.abyssnlp.functions;

import com.github.abyssnlp.models.StockPrice;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class TotalAverageMonthProcessFunction extends
    KeyedProcessFunction<String, StockPrice, Tuple2<String, Double>> {

  private static class AverageState {

    Integer count;
    Double total;
    Long lastModified;

    public AverageState(Integer count, Double total, Long lastModified) {
      this.count = count;
      this.total = total;
      this.lastModified = lastModified;
    }
  }

  private MapState<String, AverageState> state;

  @Override
  public void onTimer(long timestamp,
      KeyedProcessFunction<String, StockPrice, Tuple2<String, Double>>.OnTimerContext ctx,
      Collector<Tuple2<String, Double>> out) throws Exception {
    String key = ctx.getCurrentKey();
    AverageState current = state.get(key);

    if (timestamp >= current.lastModified + 5000) {
      out.collect(new Tuple2<>(key, current.total / current.count));
    }
  }

  @Override
  public void processElement(StockPrice stockPrice,
      KeyedProcessFunction<String, StockPrice, Tuple2<String, Double>>.Context context,
      Collector<Tuple2<String, Double>> collector) throws Exception {

    String key = context.getCurrentKey();
    Long lastModified = context.timestamp();

    if (state.contains(key)) {
      AverageState current = state.get(key);
      Integer newCount = current.count + 1;
      Double newSum = current.total + stockPrice.getClose();
      state.put(key, new AverageState(newCount, newSum, lastModified));
    } else {
      state.put(key, new AverageState(1, stockPrice.getClose(), lastModified));
    }

    // 5 seconds to check
    context.timerService().registerEventTimeTimer(lastModified + 5000);

  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    state = getRuntimeContext().getMapState(new MapStateDescriptor<>("runningCountSum",
        TypeInformation.of(new TypeHint<String>() {
        }),
        TypeInformation.of(AverageState.class)));
  }
}
