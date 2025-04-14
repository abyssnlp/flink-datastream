package com.github.abyssnlp.models;

public class StockPrice {

  private String date;
  private Double open;
  private Double high;
  private Double low;
  private Double close;
  private Integer volume;


  public StockPrice(String date, Double open, Double high, Double low, Double close,
      Integer volume) {
    this.date = date;
    this.open = open;
    this.high = high;
    this.low = low;
    this.close = close;
    this.volume = volume;
  }

  @Override
  public String toString() {
    return "StockPrice{" +
        "date='" + date + '\'' +
        ", open=" + open +
        ", high=" + high +
        ", low=" + low +
        ", close=" + close +
        ", volume=" + volume +
        '}';
  }

  public StockPrice(String line) {
    String[] fields = line.split(",");

    try {
      this.date = fields[0];
      this.open = Double.parseDouble(fields[1]);
      this.high = Double.parseDouble(fields[2]);
      this.low = Double.parseDouble(fields[3]);
      this.close = Double.parseDouble(fields[4]);
      this.volume = Integer.parseInt(fields[5]);
    } catch (NumberFormatException e) {
      this.date = fields[0];
      this.open = Double.parseDouble(fields[1]);
      this.high = Double.parseDouble(fields[2]);
      this.low = Double.parseDouble(fields[3]);
      this.close = Double.parseDouble(fields[4]);
      this.volume = Integer.parseInt(fields[6]);
    }
  }


  public StockPrice() {
  }

  public String getDate() {
    return date;
  }

  public void setDate(String date) {
    this.date = date;
  }

  public Double getOpen() {
    return open;
  }

  public void setOpen(Double open) {
    this.open = open;
  }

  public Double getHigh() {
    return high;
  }

  public void setHigh(Double high) {
    this.high = high;
  }

  public Double getLow() {
    return low;
  }

  public void setLow(Double low) {
    this.low = low;
  }

  public Double getClose() {
    return close;
  }

  public void setClose(Double close) {
    this.close = close;
  }

  public Integer getVolume() {
    return volume;
  }

  public void setVolume(Integer volume) {
    this.volume = volume;
  }
}
