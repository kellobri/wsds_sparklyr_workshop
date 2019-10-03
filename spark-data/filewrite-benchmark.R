install.packages("bench")

# EXERCISE: File Write Benchmark - 1M row write speed
numeric <- copy_to(sc, data.frame(nums = runif(10^6)))

bench::mark(
  CSV = spark_write_csv(numeric, "data.csv", mode = "overwrite"),
  JSON = spark_write_json(numeric, "data.json", mode = "overwrite"),
  Parquet = spark_write_parquet(numeric, "data.parquet", mode = "overwrite"),
  ORC = spark_write_parquet(numeric, "data.orc", mode = "overwrite"),
  iterations = 20
) %>% ggplot2::autoplot()
