install.packages("ggplot2")
install.packages("corrr")
install.packages("dbplot")
install.packages("rmarkdown")

library(sparklyr)
library(dplyr)
sc <- spark_connect(master = "local", version = "2.3")

cars <- copy_to(sc, mtcars)

# Data Wrangling
# cars can be treated like a local dataframe - use dplyr verbs
# get the mean of all columns
summarise_all(cars, mean)

# The data is not being imported to R!
# dplyr converts query to SQL and sends to Spark 
summarise_all(cars, mean) %>%
  show_query()

# EXERCISE: Group Cars by transmission type 
cars %>%
  mutate(transmission = ifelse(am == 0, "automatic", "manual")) %>%
  group_by(transmission) %>%
  summarise_all(mean)

## REFER TO: R for DS to learn dplyr, Chapter 5

# What if the operation isn't available through dplyr & sparklyr?
# Look for a built-in function available in Spark
# dplyr passes functions it doesn't recognize to the query engine as-is 

# EXERCISE: The percentile function
summarise(cars, mpg_percentile = percentile(mpg, 0.25)) %>%
  show_query()

# EXERCISE: Correlation
# The shave() function turns all duplicated results to NAs
library(corrr)
correlate(cars, use = "pairwise.complete.obs", method = "pearson") %>%
  shave() %>%
  rplot()

# EXERCISE: Visualizations: Push compute, Collect results

library(ggplot2)
# preaggregate, collect 
car_group <- cars %>%
  group_by(cyl) %>%
  summarise(mpg = sum(mpg, na.rm = TRUE)) %>%
  collect() %>%
  print()

# pass records to plot function 
ggplot(aes(as.factor(cyl), mpg), data = car_group) +
  geom_col(fill = "#999999") + coord_flip()

# EXERCISE: Helper functions for plotting remote data 
library(dbplot)
cars %>%
  dbplot_histogram(mpg, binwidth = 3) +
  labs(title = "MPG Distribution",
       subtitle = "Histogram over miles per gallon")

# EXERCISE: Scatter-like plots
dbplot_raster(cars, mpg, wt, resolution = 16)

# -------------------------------------------------------- #

# Modeling

# Linear Regression against all features, predict miles per gallon 
cars %>%
  ml_linear_regression(mpg ~ .) %>%
  summary()

# Experiment with different features
cars %>%
  ml_linear_regression(mpg ~ hp + cyl) %>%
  summary()

# EXERCISE: Caching with compute()
cached_cars <- cars %>%
  mutate(cyl = paste0("cyl_", cyl)) %>%
  compute("cached_cars")

cached_cars %>%
  ml_linear_regression(mpg ~ .) %>%
  summary()
