################################################################################
########################### MONTHLY IMPORTER, PART 1 ###########################
################################################################################



################################################################################
#### 1. LOAD LIBRARIES AND PREPARE DATES #######################################

library(tidyverse)
library(upgo)
library(strr)
library(future)

plan(multisession)
options(future.globals.maxSize = Inf)

# Detect year_month from current date
year_month <- 
  paste0(substr(Sys.Date(), 1, 4), "_", substr(Sys.Date(), 6, 7)) %>% 
  {case_when(
    substr(., 6, 7) == "01" ~ paste0(substr(., 1, 2),
                                     as.numeric(substr(., 3, 4)) - 1, 
                                     "_12"),
    substr(., 6, 7) == "10" ~ paste0(substr(., 1, 5), "09"),
    substr(., 6, 6) == "1"  ~ paste0(substr(., 1, 5), 
                                     as.numeric(substr(., 6, 7)) - 1),
    TRUE ~ paste0(substr(., 1, 6), as.numeric(substr(., 7, 7)) - 1)
    )}

# Manually specify year_month if needed
# year_month <- "2020_01"

# Derive last_month using same pattern matching
last_month <- 
  year_month %>% 
  {case_when(
    substr(., 6, 7) == "01" ~ paste0(substr(., 1, 2),
                                     as.numeric(substr(., 3, 4)) - 1, 
                                     "_12"),
    substr(., 6, 7) == "10" ~ paste0(substr(., 1, 5), "09"),
    substr(., 6, 6) == "1"  ~ paste0(substr(., 1, 5), 
                                     as.numeric(substr(., 6, 7)) - 1),
    TRUE ~ paste0(substr(., 1, 6), as.numeric(substr(., 7, 7)) - 1)
  )}


################################################################################
################################################################################



################################################################################
#### 2. DOWNLOAD FILES FROM AWS BUCKET #########################################

### Find dates for file names ##################################################

upgo_bucket <- paws::s3()

dates <- 
  c("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", 
    "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24",
    "25", "26", "27", "28", "29", "30", "31")

property_dates <-
  suppressMessages(
    map_lgl(dates, ~{
      
      date <- 
        tryCatch({
          upgo_bucket$head_object(
            Bucket = "airdna-data",
            Key = paste0(
              "McGill/All_Property_Match_",
              substr(Sys.Date(), 1, 8),
              .x,
              ".zip"))  
        }, error = function(e) FALSE)
      
      if (length(date) > 1) date <- TRUE
      
      date
      
    }))

property_name <- 
  if (sum(property_dates) == 0) NA else {
    paste0(
      "All_Property_Match_",
      substr(Sys.Date(), 1, 8),
      dates[which(property_dates)],
      ".zip"
    )}

daily_dates <-
  suppressMessages(
    map_lgl(dates, ~{
      
      date <- 
        tryCatch({
          upgo_bucket$head_object(
            Bucket = "airdna-data",
            Key = paste0(
              "McGill/All_1Month_Daily_Match_",
              substr(Sys.Date(), 1, 8),
              .x,
              ".zip"))  
        }, error = function(e) FALSE)
      
      if (length(date) > 1) date <- TRUE
      
      date
      
    }))

daily_name <-
  if (sum(daily_dates) == 0) NA else {
    paste0(
      "All_1Month_Daily_Match_",
      substr(Sys.Date(), 1, 8),
      dates[which(daily_dates)],
      ".zip"
    )}


### Download and unzip the files ###############################################

## Download property file

# Try the download up to 10 times until it succeeds
for(i in 1:10){
  try({
    upgo_aws_download(property_name, "data")
    break
  })}


## Unzip and rename property file

prop_flag %<-% {
  
  unzip(paste0("data/", property_name), exdir = "data", unzip = "unzip")
  
  file.rename(
    paste0("data/", str_sub(property_name, 1, -5), ".csv"),
    paste0("data/property_", year_month, ".csv")
  )
  
  R.utils::gzip(
    paste0("data/property_", year_month, ".csv"),
    paste0("data/property_", year_month, ".gz"),
    remove = TRUE
  )
  
  file.remove(paste0("data/", property_name))
  
  # Set flag to delay property import until future is processed
  TRUE
  
  }


## Download daily file

for(i in 1:10){
  try({
    upgo_aws_download(daily_name, "data")
    break
  })}


## Unzip and rename daily file

daily_flag %<-% {
  unzip(paste0("data/", daily_name), exdir = "data", unzip = "unzip")
  
  file.rename(
    paste0("data/", str_sub(daily_name, 1, -5), ".csv"),
    paste0("data/daily_", year_month, ".csv")
  )
  
  R.utils::gzip(
    paste0("data/daily_", year_month, ".csv"),
    paste0("data/daily_", year_month, ".gz"),
    remove = TRUE
  )
  
  file.remove(paste0("data/", daily_name))
  
  TRUE
  
  }


### Clean up ###################################################################

rm(dates, property_dates, property_name, daily_dates, daily_name, i, 
   upgo_bucket)


################################################################################
################################################################################



################################################################################
#### 3. DO INITIAL PROCESSING OF PROPERTY FILE #################################

### Read raw CSV of property file ##############################################

# Wait to proceed until prop_flag is active
if (prop_flag) {
  suppressWarnings(
    property %<-% read_csv(paste0("data/property_", year_month, ".gz"),
                           col_types = cols_only(
                             `Property ID` = col_character(),
                             `Listing Title` = col_character(),
                             `Property Type` = col_character(),
                             `Listing Type` = col_character(),
                             `Created Date` = col_date(format = ""),
                             `Last Scraped Date` = col_date(format = ""),
                             Country = col_character(),
                             Latitude = col_double(),
                             Longitude = col_double(),
                             State = col_character(),
                             City = col_character(),
                             Neighborhood = col_character(),
                             `Metropolitan Statistical Area` = col_character(),
                             `Currency Native` = col_character(),
                             Bedrooms = col_double(),
                             Bathrooms = col_double(),
                             `Max Guests` = col_double(),
                             `Response Rate` = col_double(),
                             `Airbnb Superhost` = col_logical(),
                             `HomeAway Premier Partner` = col_logical(),
                             `Cancellation Policy` = col_character(),
                             `Security Deposit (USD)` = col_double(),
                             `Cleaning Fee (USD)` = col_double(),
                             `Extra People Fee (USD)` = col_double(),
                             `Check-in Time` = col_character(),
                             `Checkout Time` = col_character(),
                             `Minimum Stay` = col_double(),
                             `Number of Reviews` = col_double(),
                             `Number of Photos` = col_double(),
                             `Instantbook Enabled` = col_logical(),
                             `Overall Rating` = col_double(),
                             `Airbnb Property ID` = col_character(),
                             `Airbnb Host ID` = col_character(),
                             `HomeAway Property ID` = col_character(),
                             `HomeAway Property Manager` = col_character()
                           ))
    )
  }


### Start async import of new daily file #######################################

if (daily_flag) {
  suppressWarnings(
    daily %<-% 
      read_csv(paste0("data/daily_", year_month, ".gz"), 
               col_types = cols_only(`Property ID` = col_character(),
                                     Date = col_date(format = ""),
                                     Status = col_character()))
    )
  }


### Process property file ######################################################

output <- 
  property %>% 
  strr_process_property()

property <- output[[1]]

write_csv(output[[2]], paste0("output/property/error_", year_month, ".csv"))
write_csv(output[[3]],
          paste0("output/property/missing_geography_", year_month, ".csv"))

rm(output, prop_flag, daily_flag)


################################################################################
################################################################################



################################################################################
#### 4. CLEAN UP PROPERTY FILE WITH DAILY FILE #################################

### Fill in missing created and scraped values from daily table on server ######

upgo_connect(property = FALSE, host = FALSE)

property <- 
  daily_all %>% 
  group_by(property_ID) %>% 
  filter(property_ID %in% !! filter(property, is.na(created))$property_ID,
         start_date == min(start_date[status != "U"], na.rm = TRUE)) %>% 
  collect() %>% 
  select(property_ID, created2 = start_date) %>% 
  left_join(property, ., by = "property_ID") %>% 
  mutate(created = if_else(is.na(created), created2, created)) %>% 
  select(-created2)

property <- 
  daily_all %>% 
  group_by(property_ID) %>% 
  filter(property_ID %in% !! filter(property, is.na(scraped))$property_ID,
         end_date == max(end_date[status != "U"], na.rm = TRUE)) %>% 
  collect() %>% 
  select(property_ID, scraped2 = end_date) %>% 
  left_join(property, ., by = "property_ID") %>% 
  mutate(scraped = if_else(is.na(scraped), scraped2, scraped)) %>% 
  select(-scraped2) %>% 
  filter(!is.na(scraped))

upgo_disconnect()


### Fill in missing created values from new daily file #########################

daily_created <- 
  daily %>% 
  set_names(c("property_ID", "date", "status")) %>% 
  filter(status != "U") %>% 
  group_by(property_ID) %>% 
  summarize(created_new = min(date))

property <- 
  property %>% 
  left_join(daily_created, by = "property_ID") %>% 
  mutate(created = if_else(is.na(created), created_new, created)) %>% 
  select(-created_new)

rm(daily, daily_created)


### Save temporary copy of property file #######################################

save(property, file = "temp_1.Rdata")


################################################################################
################################################################################



################################################################################
#### 5. PREPARE PROPERTY FILE FOR GEOGRAPHY FIXES ##############################

### Check raw regions against pre-existing region list #########################

load("data/region_list.Rdata")

new_regions <- 
  property %>% 
  filter(!is.na(region), !region %in% region_list)

region_list <- 
  sort(unique(c(region_list, unique(new_regions$region))))

save(region_list, file = "data/region_list.Rdata")


### Load location table and scrape results #####################################

load(paste0("output/property/location_table_", last_month, ".Rdata"))
load("data/scrape_results.Rdata")

# Identify new scrapes since last upload, excluding HA
new_scrapes <- 
  scrape_results %>% 
  filter(str_starts(property_ID, "ab-"),
         date >= lubridate::ymd(paste0(year_month, "_01")) + months(1))


### Replace geographies for listings which match location_table ################

property_fixed <- 
  property %>% 
  # Exclude listings with new regions
  filter(!property_ID %in% new_regions$property_ID) %>% 
  # Exclude listings with new scrapes
  filter(!property_ID %in% new_scrapes$property_ID) %>% 
  select(-country, -region, -city) %>% 
  inner_join(location_table, by = c("property_ID", "latitude", "longitude")) %>% 
  select(property_ID:housing, latitude, longitude, country:city,
         everything())


### Save temporary output ######################################################

save(property_fixed, file = "temp_2.Rdata")


### Pull out listings to be checked ############################################

property <- 
  property %>% 
  filter(!property_ID %in% property_fixed$property_ID)


### Save temporary output ######################################################

save(property, file = "temp_3.Rdata")


### Find listings without scrapes in the last month ############################

scrape_pool <-
  property %>% 
  select(property_ID:city) %>% 
  filter(!(property_ID %in% 
             filter(scrape_results, 
                    date >= as.Date(paste0(substr(year_month, 1, 4), "-", 
                                           substr(year_month, 6, 7), "-28"))
             )$property_ID))


## Prioritize listings which are new

scrape_pool_1 <- 
  scrape_pool %>% 
  filter(created >= lubridate::ymd(paste0(year_month, "_01")))

scrape_pool_2 <- 
  scrape_pool %>% 
  filter(!property_ID %in% scrape_pool_1$property_ID)


### Save output and clean up ###################################################

save(scrape_pool_1, scrape_pool_2, file = "data/scrape_pool.Rdata")

rm(property, property_fixed, location_table, new_scrapes, scrape_pool, 
   scrape_pool_1, scrape_pool_2, scrape_results, new_regions, region_list)


################################################################################
################################################################################
