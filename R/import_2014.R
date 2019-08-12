#### Importer for 2014 #########################################################

daily <- read_second("data/2014/daily_2014_processed.csv", skip = 1)
output <- prepare(daily)
  
daily_list <- output[[1]]
write_csv(output[[2]], "output/error_2014.csv")
write_csv(output[[3]], "output/missing_rows_2014.csv")
  
rm(output, daily)
  
compressed <- compress(daily_list)
save(compressed, file = "output/compressed_2014.Rdata")
rm(daily_list, compressed)
