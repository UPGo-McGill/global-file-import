#### Importer and uploader for monthly deltas ##################################

### Replace property file


### Upload daily file increment

year_month <- "2019_06"

output <- prepare(daily)

daily_list <- output[[1]]
write_csv(output[[2]], 
          paste0("output/deltas/error_", year_month, ".csv"))
write_csv(output[[3]], 
          paste0("output/deltas/missing_rows_", year_month, ".csv"))

rm(output, daily)

compressed <- compress(daily_list)
save(compressed, 
     file = paste0("output/deltas/compressed_", year_month, ".Rdata"))


### Upload review file increment