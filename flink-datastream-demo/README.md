Stream Processing with Apache Flink. Part 2
===

This is the companion code for the Unskew data [blog post](https://unskewdata.com/blog/stream-flink-2).

The stateful application uses the `Flink java client` to read from a CSV source, computes a running average and writes back to the local filesystem as partitioned parquet files. We use Java 11 for this post.

Please [write](mailto:contactunskewdata@gmail.com) to me with any questions, comments or improvements. Feel free to open an issue if you run into problems while running the application.
