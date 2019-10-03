library(sparklyr)
sc <- spark_connect(master = "local", version = "2.3")

# EXERCISE: Folder as a Table
letters_df <- data.frame(x = letters, y = 1:length(letters))

dir.create("data-csv")
write.csv(letters_df[1:3, ], "data-csv/letters1.csv", row.names = FALSE)
write.csv(letters_df[1:3, ], "data-csv/letters2.csv", row.names = FALSE)

# In R:
do.call("rbind", lapply(dir("data-csv", full.names = TRUE), read.csv))
# In Spark:
spark_read_csv(sc, "data-csv/")

# EXERCISE: Define a Schema
spec_with_r <- sapply(read.csv("data-csv/letters1.csv", nrows = 10), class)
spark_read_csv(sc, "data-csv/", columns = spec_with_r)

# EXERCISE: CSV Parsing Options 
## Creates bad test file
writeLines(c("bad", 1, 2, 3, "broken"), "bad.csv")
spark_read_csv(
  sc,
  "bad3",
  "bad.csv",
  columns = list(foo = "integer"),
  options = list(mode = "DROPMALFORMED"))

# EXERCISE: Working with JSON 
## Create a simple json file
writeLines("{'a':1, 'b': {'f1': 2, 'f3': 3}}", "data.json")
simple_json <- spark_read_json(sc, "data.json")

## Use sparklyr.nested
install.packages('sparklyr.nested')
sparklyr.nested::sdf_unnest(simple_json, "b")

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

# EXAMPLE: Connect Spark to AWS S3 
Sys.setenv(AWS_ACCESS_KEY_ID = my_key_id)
Sys.setenv(AWS_SECRET_ACCESS_KEY = my_secret_key)

sc <- spark_connect(master = "local", version = "2.3", config = list(
  sparklyr.connect.packages = "org.apache.hadoop:hadoop-aws:2.7.7"))
my_file <- spark_read_csv(sc, "my-file", path =  "s3a://my-bucket/my-file.csv")

# EXAMPLE: Map Data - Memory = FALSE
mapped_csv <- spark_read_csv(sc, "data-csv/", memory = FALSE)

mapped_csv %>%
  dplyr::select(y) %>%
  dplyr::compute("test")

