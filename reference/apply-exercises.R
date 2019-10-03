# Spark Apply 

sdf_len(sc, 3) %>% spark_apply(~ 10 * .x)


install.packages('tidytext')

sentences <- copy_to(sc, data.frame(text = c("I like apples", "I like bananas")))

sentences %>%
  spark_apply(~tidytext::unnest_tokens(.x, word, text))
