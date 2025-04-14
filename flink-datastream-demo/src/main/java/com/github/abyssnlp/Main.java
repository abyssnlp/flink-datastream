package com.github.abyssnlp;

import com.github.abyssnlp.functions.AveragePerMonthProcessFunction;
import com.github.abyssnlp.functions.StockPriceMapper;
import com.github.abyssnlp.functions.TotalAverageMonthProcessFunction;
import com.github.abyssnlp.models.StockPrice;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;

public class Main {

  private static class MonthAverage {

    String date;
    Double average;

    public MonthAverage(String date, Double average) {
      this.date = date;
      this.average = average;
    }

    @Override
    public String toString() {
      return "MonthAverage{" +
          "date='" + date + '\'' +
          ", average=" + average +
          '}';
    }
  }

  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setRuntimeMode(RuntimeExecutionMode.BATCH);

    FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(),
        new Path("/opt/data/msft.csv")).build();

    // sink to parquet
    FileSink<MonthAverage> sink = FileSink
        .forBulkFormat(new Path("/opt/data/out"),
            AvroParquetWriters.forReflectRecord(MonthAverage.class))
        .withBucketAssigner(new BucketAssigner<MonthAverage, String>() {
          @Override
          public String getBucketId(MonthAverage monthAverage, Context context) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            return String.valueOf(LocalDate.parse(monthAverage.date, formatter).getYear());
          }

          @Override
          public SimpleVersionedSerializer<String> getSerializer() {
            return null;
          }
        })
        .build();

    DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(),
        "msft-source");

    KeyedStream<StockPrice, String> keyedStream = stream.filter(
            line -> !line.toLowerCase().startsWith("date")).map(new StockPriceMapper())
        .keyBy(
            record -> {
              DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
              return LocalDate.parse(record.getDate(), formatter).withDayOfMonth(1).toString();
            });

    // Write running average as the stream arrives
    keyedStream
        .process(new AveragePerMonthProcessFunction())
        .print();

    // Calculate the average over all months once (batch)
    // option 1: Use table API
    // option 2: Get it from the previous stream by using KeyedProcess onTimer to emit when it has not been
    // updated in 5 seconds

    DataStream<MonthAverage> intermediateStream = keyedStream
        .process(new TotalAverageMonthProcessFunction())
        .map(record -> new MonthAverage(record.f0, record.f1));

    // print to stdout and sink as parquet
    intermediateStream.print();
    intermediateStream.sinkTo(sink);

    env.execute();
  }
}
