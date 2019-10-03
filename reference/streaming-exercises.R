library(dplyr)
library(sparklyr)
sc <- sparklyr::spark_connect(master = "local")

# EXERCISE: Simple Spark Stream Processing
install.packages("future")
dir.create("source") 

stream <- stream_read_text(sc, "source/") %>%
  stream_write_text("destination/")

future::future(stream_generate_test(interval = 0.5))
stream_view(stream)

stream_stop(stream)

spark_disconnect(sc)
unlink(c("source", "destination", "cars-stream",
         "car-round", "shiny/shiny-stream"), recursive = TRUE)

# EXERCISE: Modeling Pipeline
cars <- copy_to(sc, mtcars)
model <- ml_pipeline(sc) %>%
  ft_binarizer("mpg", "over_30", 30) %>%
  ft_r_formula(over_30 ~ wt) %>%
  ml_logistic_regression() %>%
  ml_fit(cars)

# generate stream of mtcars
future::future(stream_generate_test(mtcars, "cars-stream", iterations = 5))
ml_transform(model, stream_read_csv(sc, "cars-stream"))

# EXERCISE: Stream Spark Apply 

stream_read_csv(sc, "cars-stream") %>%
  select(mpg) %>%
  spark_apply(~ round(.x), mpg = "integer") %>%
  stream_write_csv("cars-round")

# EXERCISE: Shiny Streaming
stream_generate_test(datasets::iris, "shiny-stream", rep(5, 10^3))
