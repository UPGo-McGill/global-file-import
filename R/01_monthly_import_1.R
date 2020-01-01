################################################################################
########################### MONTHLY IMPORTER, PART 1 ###########################
################################################################################



################################################################################
#### 1. LOAD LIBRARIES AND PREPARE DATES #######################################

library(tidyverse)
library(sf)
library(strr)
library(upgo)

# Detect year_month from current date
year_month <- 
  paste0(substr(Sys.Date(), 1, 4), "_", substr(Sys.Date(), 6, 7)) %>% 
  {case_when(
    substr(., 6, 7) == "01" ~ paste0(substr(., 1, 3),
                                     as.numeric(substr(., 4, 4)) - 1, 
                                     "_12"),
    substr(., 6, 7) == "10" ~ paste0(substr(., 1, 5), "09"),
    substr(., 6, 6) == "1"  ~ paste0(substr(., 1, 5), 
                                     as.numeric(substr(., 6, 7)) - 1),
    TRUE ~ paste0(substr(., 1, 6), as.numeric(substr(., 7, 7)) - 1)
    )}

# Manually specify year_month if needed
#year_month <- "2019_11"

# Derive last_month using same pattern matching
last_month <- 
  year_month %>% 
  {case_when(
    substr(., 6, 7) == "01" ~ paste0(substr(., 1, 3),
                                     as.numeric(substr(., 4, 4)) - 1, 
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

### Retrieve and process property file #########################################

## Find date for property file name

property_dates <- 
  suppressMessages(
    map_lgl(10:31, ~{
      aws.s3::head_object(
        bucket = "airdna-data/McGill",
        region = "us-west-1",
        check_region = FALSE,
        object = paste0(
          "All_Property_Match_",
          substr(Sys.Date(), 1, 8),
          .x,
          ".zip"))[[1]]
    }))

property_name <- 
  paste0(
    "All_Property_Match_",
    substr(Sys.Date(), 1, 8),
    (10:31)[which(property_dates)],
    ".zip"
  )


## Download, unzip, rezip and rename the property file

# Try the download up to 10 times until it succeeds
for(i in 1:10){
  try({
    upgo_aws_download(property_name, "data")
    break
  })}

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


## Clean up

file.remove(paste0("data/", property_name))

rm(property_dates, property_name)


### Retrieve and process daily file ############################################

## Find date for daily file name

daily_dates <- 
  suppressMessages(
    map_lgl(10:31, ~{
      aws.s3::head_object(
        bucket = "airdna-data/McGill",
        region = "us-west-1",
        check_region = FALSE,
        object = paste0(
          "All_1Month_Daily_Match_",
          substr(Sys.Date(), 1, 8),
          .x,
          ".zip"))[[1]]
    }))

daily_name <- 
  paste0(
    "All_1Month_Daily_Match_",
    substr(Sys.Date(), 1, 8),
    (10:31)[which(daily_dates)],
    ".zip"
  )


## Download, unzip, rezip and rename the daily file

for(i in 1:10){
  try({
    upgo_aws_download(daily_name, "data")
    break
  })}

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


## Clean up

file.remove(paste0("data/", daily_name))

rm(daily_dates, daily_name, i)


################################################################################
################################################################################



################################################################################
#### 3. DO INITIAL PROCESSING OF PROPERTY FILE #################################

### Read raw CSV of property file ##############################################

property2 <- read_csv(paste0("data/property_", year_month, ".gz"),
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
                       `Airbnb Property ID` = col_double(),
                       `Airbnb Host ID` = col_double(),
                       `HomeAway Property ID` = col_character(),
                       `HomeAway Property Manager` = col_character()
                     )) %>% 
  set_names(c("property_ID", "listing_title", "property_type", "listing_type",
              "created", "scraped", "country", "latitude", "longitude",
              "region", "city", "neighbourhood", "metro_area", "currency",
              "bedrooms", "bathrooms", "max_guests", "response_rate",
              "superhost", "premier_partner", "cancellation", 
              "security_deposit", "cleaning_fee", "extra_people_fee",
              "check_in_time", "check_out_time", "minimum_stay", "num_reviews",
              "num_photos", "instant_book", "rating", "ab_property", "ab_host",
              "ha_property", "ha_host"))


### Produce error files ########################################################

error <- 
  problems(property) %>% 
  filter(expected != "56 columns") %>% 
  pull(row) %>% 
  {property[.,]}

if (nrow(error) > 0) {
  property <- 
    problems(property) %>% 
    filter(expected != "56 columns") %>% 
    pull(row) %>% 
    {property[-.,]}
}

error <- 
  property %>% 
  filter(is.na(property_ID) | is.na(listing_type)) %>% 
  rbind(error)

property <- 
  property %>% 
  filter(!is.na(property_ID), !is.na(listing_type))

error <- 
  property %>% 
  filter(!str_starts(property_ID, "ab-"), !str_starts(property_ID, "ha-")) %>% 
  rbind(error)

property <- 
  property %>% 
  filter(str_starts(property_ID, "ab-") | str_starts(property_ID, "ha-"))

missing_geography <- 
  property %>% 
  filter(is.na(latitude) | is.na(longitude))

property <- 
  property %>% 
  filter(!is.na(latitude), !is.na(longitude))

write_csv(error, paste0("output/property/error_", year_month, ".csv"))
write_csv(missing_geography,
          paste0("output/property/missing_geography_", year_month, ".csv"))

rm(error, missing_geography)


### Replace problematic characters #############################################

property <- 
  property %>% 
  mutate(listing_title = str_replace_all(
    listing_title, c('\n' = "", '\r' = "", '\"' = "", "\'" = "")))


################################################################################
################################################################################



################################################################################
#### 4. CLEAN UP PROPERTY FILE FIELDS ##########################################

### Fill in missing created and scraped values from daily table on server ######

upgo::upgo_connect(property = FALSE, multi = FALSE)

property <- 
  daily_all %>% 
  group_by(property_ID) %>% 
  filter(property_ID %in% !! filter(property, is.na(created))$property_ID,
         start_date == min(start_date[status != "U"], na.rm = TRUE)) %>% 
  collect() %>% 
  select(property_ID, created2 = start_date) %>% 
  left_join(property, .) %>% 
  mutate(created = if_else(is.na(created), created2, created)) %>% 
  select(-created2)

property <- 
  daily_all %>% 
  group_by(property_ID) %>% 
  filter(property_ID %in% !! filter(property, is.na(scraped))$property_ID,
         end_date == max(end_date[status != "U"], na.rm = TRUE)) %>% 
  collect() %>% 
  select(property_ID, scraped2 = end_date) %>% 
  left_join(property, .) %>% 
  mutate(scraped = if_else(is.na(scraped), scraped2, scraped)) %>% 
  select(-scraped2)

upgo::upgo_disconnect()


### Import new daily file ######################################################

daily <- read_csv(paste0("data/daily_", year_month, ".zip"), 
                  col_types = cols_only(`Property ID` = col_character(),
                                        Date = col_date(format = ""),
                                        Status = col_character(),
                                        `Booked Date` = col_date(format = ""),
                                        `Price (USD)` = col_integer(),
                                        `Reservation ID` = col_integer())) %>% 
  set_names(c("property_ID", "date", "status", "booked_date", "price", 
              "res_ID"))


### Save daily_length for later ################################################

daily_length <- nrow(daily) - 1
save(daily_length, file = "data/daily_length.Rdata")


### Fill in missing created values from new daily file #########################

daily_created <- 
  daily %>% 
  filter(status != "U") %>% 
  group_by(property_ID) %>% 
  summarize(created_new = min(date))

property <- 
  property %>% 
  left_join(daily_created) %>% 
  mutate(created = if_else(is.na(created), created_new, created)) %>% 
  select(-created_new)

rm(daily, daily_created, daily_length)


### Add host_ID field ##########################################################

property <- 
  property %>% 
  mutate(host_ID = if_else(!is.na(ab_host), as.character(ab_host), ha_host))


### Add housing field ##########################################################

property <- 
  property %>% 
  strr_housing() %>% 
  select(property_ID, host_ID, listing_title:scraped, housing,
         latitude:longitude, country, region:ha_host)


### Save temporary copy of property file #######################################

save(property, file ="temp_1.Rdata")


################################################################################
################################################################################



################################################################################
#### 5. PREPARE PROPERTY FILE FOR GEOGRAPHY FIXES ##############################

### Check raw regions against pre-existing region list #########################

load("data/region_list.Rdata")

new_regions <- 
  property %>% 
  filter(!is.na(region, !region %in% region_list))

region_list <- 
  sort(unique(c(region_list, unique(new_regions$region))))

save(region_list, file = "data/region_list.Rdata")


### Load location table and scrape results #####################################

load(paste0("output/property/location_table_", last_month, ".Rdata"))
load("data/scrape_results.Rdata")


### Replace geographies for listings which match location_table ################

property_fixed <- 
  property %>% 
  # Exclude listings with new regions
  filter(!property_ID %in% new_regions$property_ID) %>% 
  select(-country, -region, -city) %>% 
  inner_join(location_table) %>% 
  select(property_ID:housing, latitude, longitude, country:city,
         everything())


### Pull out listings to be checked ############################################

property <- 
  property %>% 
  filter(!property_ID %in% property_fixed$property_ID)


## Find listings without scrapes in the last month

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

save(scrape_pool_1, scrape_pool_2, file = "data/scrape_pool.Rdata")


### Rename country names to match actual Airbnb usage ##########################

property <- 
  property %>% 
  mutate(country = case_when(
    country == "Bosnia-Herzegovina"           ~ "Bosnia and Herzegovina",
    country == "Brunei Darussalam"            ~ "Brunei",
    country == "Côte d'Ivoire - Republic of"  ~ "Ivory Coast",
    country == "Curacao"                      ~ "Curaçao",
    country == "Falkland Islands"             ~ "Falkland Islands (Malvinas)",
    country == "Lao"                          ~ "Laos",
    country == "Macedonia - Republic of"      ~ "Macedonia",
    country == "Palestine"                    ~ "Palestinian Territories",
    country == "Republic of Congo"            ~ "Congo",
    country == "Saint-Martin"                 ~ "Saint Martin",
    region  == "Bhutan"                       ~ "Bhutan",
    region  == "Puerto Rico"                  ~ "Puerto Rico",
    region  == "United States Virgin Islands" ~ "U.S. Virgin Islands",
    TRUE ~ country
  ))


### Save temporary output ######################################################

save(property_fixed, file = "temp_2.Rdata")
save(property, file = "temp_3.Rdata")

rm(property_fixed, location_table, scrape_pool, scrape_pool_1, scrape_pool_2,
   new_regions, region_list)


################################################################################
################################################################################
