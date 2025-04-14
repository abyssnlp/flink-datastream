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


public class AveragePerMonthProcessFunction extends
    KeyedProcessFunction<String, StockPrice, Tuple2<String, Double>> {

  private MapState<String, Tuple2<Integer, Double>> state;

  @Override
  public void processElement(StockPrice stockPrice,
      KeyedProcessFunction<String, StockPrice, Tuple2<String, Double>>.Context context,
      Collector<Tuple2<String, Double>> collector) throws Exception {

    String key = context.getCurrentKey();

    if (state.contains(key)) {
      Tuple2<Integer, Double> values = state.get(key);
      Integer newCount = values.f0 + 1;
      Double newSum = values.f1 + stockPrice.getClose();
      state.put(key, new Tuple2<>(newCount, newSum));
      collector.collect(new Tuple2<>(key, newSum / newCount));
    } else {
      state.put(key, new Tuple2<>(1, stockPrice.getClose()));
      collector.collect(new Tuple2<>(key, stockPrice.getClose()));
    }

  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    state = getRuntimeContext().getMapState(new MapStateDescriptor<>("runningCountSum",
        TypeInformation.of(new TypeHint<String>() {
        }),
        TypeInformation.of(new TypeHint<Tuple2<Integer, Double>>() {
        })));
  }
}
