package com.github.abyssnlp.functions;

import com.github.abyssnlp.models.StockPrice;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

public class StockPriceMapper extends RichMapFunction<String, StockPrice> {

  @Override
  public StockPrice map(String s) throws Exception {
    return new StockPrice(s);
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
  }
}
