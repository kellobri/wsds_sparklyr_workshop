#
# Chapter 8: Data
# Copying Data - HDFS
#

dir.create("largefile.txt")
write.table(matrix(rnorm(10 * 10^6), ncol = 10), "largefile.txt/1",
            append = T, col.names = F, row.names = F)
for (i in 2:30)
  file.copy("largefile.txt/1", paste("largefile.txt/", i))

# hadoop fs -copyFromLocal largefile.txt largefile.txt

spark_read_text(sc, "largefile.txt", memory = FALSE)

# collect() has a similar limitation in that it can collect only datasets that fit your driver memory; 
# however, if you had to extract a large dataset from Spark through the driver node, 
# you could use specialized tools provided by the distributed storage.

# hadoop fs -copyToLocal largefile.txt largefile.txt

# Clean up

unlink("largefile.txt", recursive = TRUE)