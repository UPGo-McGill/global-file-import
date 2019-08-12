#### Importer for 2019 #########################################################

n <- 1

for (char in c("aa", "ab", "ac", "ad", "ae", "af", "ag", "ah", "ai", "aj", "ak",
               "al", "am", "an")) {
  
  x <- 0
  if (char == "aa") x <- 1
  
  daily <- read_second(paste0("data/2019/x", char, ".csv"), skip = x)
  output <- prepare(daily)
  
  daily_list <- output[[1]]
  write_csv(output[[2]], paste0("output/error_2019_", n, ".csv"))
  write_csv(output[[3]], paste0("output/missing_rows_2019_", n, ".csv"))
  
  rm(output, daily)
  
  print(Sys.time())
  
  compressed <- compress(daily_list)
  save(compressed, file = paste0("output/compressed_2019_", n, ".Rdata"))
  rm(daily_list, compressed)
  
  print(n)
  n <- n + 1
  
  print(Sys.time())
}
