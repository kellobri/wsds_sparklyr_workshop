
# EXERCISE: Install Spark and Connect
library(sparklyr)
spark_install("2.3")
spark_installed_versions()
sc <- spark_connect(master = "local", version = "2.3")
# (inspect the connection object)
spark_disconnect(sc)

# EXERCISE: RStudio Connections Pane
# Load a dataset into Spark
cars <- copy_to(sc, mtcars)

# EXERCISE: Construct a query DBI & dpylr
library(DBI)
dbGetQuery(sc, "SELECT count(*) FROM mtcars")

library(dplyr)
count(cars)

# Start by loadidng sparklyr
library(sparklyr)

# Install Spark Locally
spark_install("2.3")

# Display all the versions of Spark available for installation
spark_available_versions()
# Display which versions have been installed
spark_installed_versions()

# Uninstall a specific version: spark_uninstall(version = "1.6.3", hadoop = "2.6")

# What is a local Spark cluster good for?
# - getting started
# - testing code
# - troubleshooting

# Connect - Opens the Connections pane!
# spark_connect() retrieves an active Spark connection, used to execute Spark commands
sc <- spark_connect(master = "local", version = "2.3")

# Execute Commands
# Load a dataset into Spark
cars <- copy_to(sc, mtcars)

#
# TODO: Explore Spark Web Interface
#

# ANALYSIS

# DBI
library(DBI)
dbGetQuery(sc, "SELECT count(*) FROM mtcars")

# dplyr
library(dplyr)
count(cars)

# Plot Horsepower versus miles per gallon
select(cars, hp, mpg) %>%
  sample_n(100) %>%
  collect() %>%
  plot()

# MODELING 

model <- ml_linear_regression(cars, mpg ~ hp)
model

# Use model to predict values not in the original dataset 

model %>%
  ml_predict(copy_to(sc, data.frame(hp = 250 + 10 * 1:10))) %>%
  transmute(hp = hp, mpg = prediction) %>%
  full_join(select(cars, hp, mpg)) %>%
  collect() %>%
  plot()

# MOVE TO CLUSTER?

# spark_write_csv(cars, "cars.csv")
# spark_read_csv(sc, "cars.csv")

# DISTRIBUTED R 

cars %>% spark_apply(~round(.x))

# STREAMING 

dir.create("input")
write.csv(mtcars, "input/cars_1.csv", row.names = F)
stream <- stream_read_csv(sc, "input/") %>%
  select(mpg, cyl, disp) %>%
  stream_write_csv("output/")

# check contents of output 
dir("output", pattern = ".csv")

# write more data into the stream source
write.csv(mtcars, "input/cars_2.csv", row.names = F)

# stop the stream!
stream_stop(stream)

# LOGS 

spark_log(sc)
spark_log(sc, filter = "sparklyr")

# DISCONNECTING 

spark_disconnect(sc)