################################################################################
############################### MONTHLY IMPORTER ###############################
################################################################################



################################################################################
#### 1. LOAD LIBRARIES #########################################################

library(tidyverse)
library(sf)
library(strr)
library(upgo)
library(RPostgres)

year_month <- "2019_11"

last_month <- case_when(
  substr(year_month, 6, 7) == "01" ~ paste0(substr(year_month, 1, 3),
                                            as.numeric(substr(year_month, 4, 
                                                              4)) - 1, "_12"),
  substr(year_month, 6, 7) == "10" ~ paste0(substr(year_month, 1, 5), "09"),
  substr(year_month, 6, 6) == "1"  ~ paste0(substr(year_month, 1, 5), 
                                            as.numeric(substr(year_month, 6,
                                                              7)) - 1),
  TRUE ~ paste0(substr(year_month, 1, 6), 
                as.numeric(substr(year_month, 7, 7)) - 1)
)


################################################################################
################################################################################



################################################################################
#### 2. DO INITIAL PROCESSING OF PROPERTY FILE #################################

### Read raw CSV of property file ##############################################

property <- read_csv(paste0("data/property_", year_month, ".csv"),
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
#### 3. CLEAN UP PROPERTY FILE FIELDS ##########################################

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

daily <- read_csv(paste0("data/daily_", year_month, ".csv"), 
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
#### 4. PREPARE PROPERTY FILE FOR GEOGRAPHY FIXES ##############################

### Load location table and scrape results #####################################

load(paste0("output/property/location_table_", last_month, ".Rdata"))
load("data/scrape_results.Rdata")
load("data/ongoing_results.Rdata")

scrape_results <- 
  bind_rows(scrape_results, ongoing_results) %>% 
  filter(!is.na(raw)) %>% 
  distinct() %>% 
  group_by(property_ID) %>% 
  filter(date == max(date)) %>% 
  ungroup()

save(scrape_results, file = "data/scrape_results.Rdata")


### Replace geographies for all listings which match location_table ############

property_fixed <- 
  property %>% 
  select(-country, -region, -city) %>% 
  inner_join(location_table) %>% 
  select(property_ID:housing, latitude, longitude, country:city,
         everything())


### Pull out listings to be checked ############################################

property <- 
  property %>% 
  anti_join(select(property_fixed, property_ID))


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

save(property_fixed, property, file = "temp_2.Rdata")

rm(property_fixed, location_table, scrape_pool)


################################################################################
################################################################################


################################################################################
#### 5. SCRAPE LOCATION OF NEW LISTINGS ########################################

### Load scrape pool and initialize server #####################################

load("data/scrape_pool.Rdata")
load("data/scrape_results.Rdata")
load("data/ongoing_results.Rdata")

ongoing_results_4 <- ongoing_results_3[0,]

rD <- upgo_scrape_connect()

scrape_pool_3 <- 
  scrape_pool_1 %>% slice(500001:720000)

scrape_pool_4 <- 
  scrape_pool_1 %>% slice(200001:500000)



### While loop to perform scrape incrementally #################################

n <- 1

while (nrow(filter(scrape_pool_4, 
                   !property_ID %in% scrape_results$property_ID,
                   !property_ID %in% ongoing_results_4$property_ID)) > 0 & 
       n < 200) {

  n <- n + 1
    
  scrape_no_matches <- 
    scrape_pool_4 %>% 
    filter(!property_ID %in% scrape_results$property_ID,
           !property_ID %in% ongoing_results_4$property_ID) %>% 
    dplyr::slice(1:10000) %>% 
    upgo_scrape_location(cores = 4)
  
  ongoing_results_4 <- 
    ongoing_results_4 %>% 
    rbind(scrape_no_matches) %>% 
    filter(!is.na(raw)) %>% 
    distinct()
  
  save(ongoing_results_4, file = "data/ongoing_results_4.Rdata")  
  
}

ongoing_results_4 <- 
  ongoing_results_4 %>% 
  rbind(.temp_results) %>% 
  filter(!is.na(raw)) %>% 
  distinct()


### Clean up ###################################################################

scrape_results <- 
  scrape_results %>% 
  filter(!property_ID %in% new_scrape_results$property_ID) %>% 
  rbind(new_scrape_results)

upgo_scrape_disconnect()

save(scrape_results, file = "data/scrape_results.Rdata")  

file.remove("data/new_scrape_results.Rdata")

rm(scrape_no_matches, new_scrape_results)


################################################################################
################################################################################



################################################################################
#### 6. GEOCODE COUNTRY FIELD IN PROPERTY FILE #################################

### Load country boundaries ####################################################

load("data/country_boundaries.Rdata")

countries_active <- 
  countries %>% 
  filter(name %in% {property %>% 
      filter(!is.na(country)) %>% 
      pull(country) %>% 
      unique() %>% 
      sort()})


### Check listing intersection with country field entry ########################

## Create list of subdivided polygons for each country with listings

country_list <- 
  countries_active %>% 
  group_split(name) %>% 
  map(~{
    .x %>% 
      lwgeom::st_subdivide(100) %>% 
      st_collection_extract("POLYGON")
  })


## Create paired list of listings and polygons for each country

pair_list <- 
  map2({property %>% filter(!is.na(country)) %>% group_split(country)}, 
       country_list, 
       list)

pair_list <- 
  pair_list[order(map_int(pair_list, ~{nrow(.x[[1]])}))]


## Calculate intersections of listings with specified country

country_matches_1 <-
  pair_list %>% 
  pbapply::pblapply(function(x) {
    x[[1]] %>% 
      st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%  
      st_transform(3857) %>% 
      st_intersection(x[[2]])}, cl = 5) %>% 
  map(~{
    .x %>% 
      st_drop_geometry() %>% 
      select(property_ID, country, region, city)
  }) %>% 
  bind_rows() %>% 
  distinct(property_ID, .keep_all = TRUE)

no_matches <- 
  property %>% 
  filter(!property_ID %in% country_matches_1$property_ID)


### Save temporary output ######################################################

save(country_matches_1, no_matches, file = "temp_3.Rdata")
rm(countries_active, country_list, pair_list)


################################################################################
################################################################################



################################################################################
#### 7. SECONDARY GEOGRAPHY TESTING ############################################

### Run a match against all countries for the listings with no matches #########

no_matches_list <- 
  no_matches %>%
  split(1:(nrow(.)/1000))

country_matches_2 <- 
  no_matches_list %>% 
  pbapply::pblapply(function(x) {
    x %>% 
      st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%  
      st_transform(3857) %>% 
      st_intersection(countries_sub)
  }, cl = 5) %>% 
  map(~{
    .x %>% 
      st_drop_geometry() %>% 
      select(property_ID, country, region, city, name)
  }) %>% 
  bind_rows()

no_matches <- 
  no_matches %>% 
  filter(!property_ID %in% country_matches_2$property_ID)


### Save temporary output ######################################################

save(country_matches_2, no_matches, file = "temp_4.Rdata")


### Third try, against GADM 5 km borders #######################################

no_matches_list <- 
  no_matches %>%
  split(1:(nrow(.)/1000))

country_matches_3 <- 
  no_matches_list %>% 
  pbapply::pblapply(function(x) {
    x %>% 
      st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%  
      st_transform(3857) %>% 
      st_intersection(country_buffer_sub)
  }, cl = 5) %>% 
  map(~{
    .x %>% 
      st_drop_geometry() %>% 
      select(property_ID, country, region, city, name)
  }) %>% 
  bind_rows()


## Decide ties based on closest distance

ties <- 
  country_matches_3 %>% 
  group_by(property_ID) %>% 
  filter(n() > 1)

ties_list <- 
  ties %>% 
  summarize(candidates = list(name)) %>% 
  split(1:100) %>% 
  pbapply::pblapply(function(x) {
    x %>% 
      mutate(winner = map2_chr(property_ID, candidates, ~{
        index <- 
          no_matches %>% 
          filter(property_ID == .x) %>% 
          st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%  
          st_transform(3857) %>% 
          st_nearest_feature(countries %>% filter(name %in% unlist(.y)))
        (countries %>% filter(name %in% unlist(.y)))[index,]$name
      }))}, cl = 5)

country_matches_3 <- 
  ties %>% 
  ungroup() %>% 
  arrange(property_ID) %>% 
  group_by(property_ID, country, region, city) %>% 
  summarize() %>% 
  ungroup() %>% 
  left_join(bind_rows(ties_list)) %>% 
  select(-candidates) %>% 
  rename(name = winner) %>% 
  rbind(country_matches_3 %>% group_by(property_ID) %>% filter(n() == 1) 
        %>% ungroup())

no_matches <- 
  no_matches %>% 
  filter(!property_ID %in% country_matches_3$property_ID)


### Save temporary output ######################################################

save(country_matches_3, no_matches, ties, file = "temp_5.Rdata")

rm(ties_list, no_matches_list)



### Fourth try, with rnaturalearth boundary files ##############################

no_matches_list <- 
  no_matches %>%
  split(1:(nrow(.)/100))

country_matches_4 <- 
  no_matches_list %>% 
  pbapply::pblapply(function(x) {
    x %>% 
      st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%  
      st_transform(3857) %>% 
      st_intersection(countries_2_sub)
  }, cl = 4) %>% 
  map(~{
    .x %>% 
      st_drop_geometry() %>% 
      select(property_ID, country, region, city, name)
  }) %>% bind_rows()

no_matches <- 
  no_matches %>% 
  filter(!property_ID %in% country_matches_4$property_ID)


### Save temporary output ######################################################

save(country_matches_4, no_matches, file = "temp_6.Rdata")


### Final try, with 5 km buffers around rnaturalearth boundary files ###########

no_matches_list <- 
  no_matches %>%
  split(1:(nrow(.)/100))

country_matches_5 <- 
  no_matches_list %>% 
  pbapply::pblapply(function(x) {
    x %>% 
      st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%  
      st_transform(3857) %>% 
      st_intersection(country_2_buffer_sub)
  }, cl = 4) %>% 
  map(~{
    .x %>% 
      st_drop_geometry() %>% 
      select(property_ID, country, region, city, name)
  }) %>% bind_rows()


## Decide ties based on closest distance

ties_2 <- 
  country_matches_5 %>% 
  group_by(property_ID) %>% 
  filter(n() > 1) 

if (nrow(ties_2) != 0) {
  ties_list <- 
    ties_2 %>% 
    summarize(candidates = list(name)) %>% 
    split(1:100) %>% 
    pbapply::pblapply(function(x) {
      x %>% 
        mutate(winner = map2_chr(property_ID, candidates, ~{
          index <- 
            no_matches %>% 
            filter(property_ID == .x) %>% 
            st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%  
            st_transform(3857) %>% 
            st_nearest_feature(countries_2 %>% filter(name %in% unlist(.y)))
          (countries_2 %>% filter(name %in% unlist(.y)))[index,]$name
        }))}, cl = 7)
  
  country_matches_5 <- 
    ties_2 %>% 
    ungroup() %>% 
    arrange(property_ID) %>% 
    group_by(property_ID, country, region, city) %>% 
    summarize() %>% 
    ungroup() %>% 
    left_join(bind_rows(ties_list)) %>% 
    select(-candidates) %>% 
    rename(name = winner) %>% 
    rbind(country_matches_5 %>% group_by(property_ID) %>% filter(n() == 1) 
          %>% ungroup())
  
}

ties <- 
  ties %>% 
  ungroup() %>% 
  rbind(ungroup(ties_2))

no_matches <- 
  no_matches %>% 
  filter(!property_ID %in% country_matches_5$property_ID)


### Save temporary output ######################################################

save(country_matches_5, ties, no_matches, file = "temp_7.Rdata")

rm(no_matches_list, ties_list, ties_2, countries, countries_2, countries_2_sub, 
   countries_sub, country_2_buffer_list, country_buffer_list, ties)


### Assemble geocoded locations ################################################

country_matches <- 
  bind_rows(
    mutate(country_matches_1, name = country),
    country_matches_2,
    country_matches_3,
    country_matches_4,
    country_matches_5,
    no_matches %>% 
      select(property_ID) %>% 
      mutate(country = NA_character_,
             region = NA_character_,
             city = NA_character_,
             name = NA_character_)
  ) %>% 
  rename(country_geo = name)


### Save temporary output ######################################################

save(country_matches, file = "temp_8.Rdata")

rm(country_matches_1, country_matches_2, country_matches_3, country_matches_4,
   country_matches_5, no_matches)


################################################################################
################################################################################



################################################################################
#### 8. RECONCILE SCRAPED AND GEOLOCATED COUNTRY NAMES #########################

### Load scrape results ########################################################

load("data/scrape_results.Rdata")
load("data/ongoing_results.Rdata")


scrape_results <- 
  bind_rows(scrape_results, ongoing_results) %>% 
  distinct()


### Produce comparison table ###################################################

country_comparison <- 
  country_matches %>% 
  left_join(scrape_results %>% 
              filter(!is.na(raw)) %>% 
              select(property_ID, country_scrape = country)) %>% 
  # Deal with weird islands
  mutate(
    country_scrape = if_else(country_scrape == "France" & country_geo %in% c(
      "French Guiana", "Guadeloupe", "Martinique", "Mayotte", "Réunion", 
      "Saint Barthélemy", "Saint Martin", "Suriname"), country_geo, 
      country_scrape),
    country_scrape = if_else(country_scrape == "United Kingdom" & 
                               country_geo == "Guernsey", country_geo, 
                             country_scrape),
    country_scrape = if_else(country_scrape == "United States" &
                               country_geo == "U.S. Virgin Islands",
                             country_geo, country_scrape),
    country_scrape = if_else(country_scrape == "China" & 
                               country_geo == "Taiwan" &
                               region == "Fujian", country_geo, country_scrape))


## Find listings with scrapes which disagree with geolocation

country_no_agree <- 
  country_comparison %>% 
  filter(!is.na(country_scrape), !is.na(country_geo), 
         country_geo != country_scrape)


### Test scraped countries against 5 km buffers ################################

## Intersect country_no_agree with GADM buffers

country_no_agree_valid <- 
  property %>% 
  filter(property_ID %in% country_no_agree$property_ID) %>% 
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%  
  st_transform(3857) %>% 
  st_intersection(country_buffer_sub) %>% 
  st_drop_geometry() %>% 
  select(property_ID, country_scrape = name) %>% 
  inner_join(country_no_agree, .)

# Otherwise use the geolocated country name

country_no_agree <- 
  country_no_agree %>% 
  filter(!property_ID %in% country_no_agree_valid$property_ID)


## Use rnaturalearth as second try
  
country_no_agree_valid_2 <- 
  property %>% 
  filter(property_ID %in% country_no_agree$property_ID) %>% 
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%  
  st_transform(3857) %>% 
  st_intersection(country_2_buffer_sub) %>% 
  st_drop_geometry() %>% 
  select(property_ID, country_scrape = name) %>% 
  inner_join(country_no_agree, .) 
 
country_no_agree <- 
  country_no_agree %>% 
  filter(!property_ID %in% country_no_agree_valid_2$property_ID)


### Reassemble results #########################################################

## For validated scrapes, use scraped location, otherwise use geolocation

country_fixed <-
  country_comparison %>% 
  filter(!property_ID %in% country_no_agree_valid$property_ID,
         !property_ID %in% country_no_agree_valid_2$property_ID) %>% 
  mutate(country = country_geo) %>% 
  mutate(region = if_else(is.na(country), NA_character_, region),
         city = if_else(is.na(country), NA_character_, city)) %>% 
  rbind(
    country_no_agree_valid %>% mutate(country = country_scrape),
    country_no_agree_valid_2 %>% mutate(country = country_scrape)) %>% 
  select(-country_geo, -country_scrape)
  

### Reassemble full location_table #############################################

location_table <- 
  country_fixed %>% 
  left_join(select(property, property_ID, latitude, longitude)) %>% 
  select(property_ID, latitude, longitude, country, region, city)


### Join location table back into property file ################################

property <-
  property %>% 
  select(-country, -region, -city) %>%
  # Inner join with length checking to catch mistakes
  inner_join(location_table) %>% 
  {
    if (nrow(.) == nrow(property)) {
      select(., property_ID:longitude, country:city, everything())
    } else {
      stop("Rows are missing from joined output.")
    }}


### Save temporary output ######################################################

save(property, location_table, file = "temp_9.Rdata")

rm(country_2_buffer_sub, country_buffer_sub, country_comparison, country_fixed,
   country_matches, country_no_agree, country_no_agree_valid, 
   country_no_agree_valid_2, location_table, ongoing_results)


################################################################################
################################################################################



################################################################################
#### 9. FILL IN MISSING REGIONS IN CANADA ######################################

### Load geometry ##############################################################

province_names <- c("Alberta", "British Columbia", "Manitoba", "New Brunswick",
                    "Newfoundland and Labrador", "Northwest Territories", 
                    "Nova Scotia", "Nunavut", "Ontario", "Prince Edward Island",
                    "Québec", "Saskatchewan", "Yukon")

provinces <- cancensus::get_census("CA16", regions = list(C = "01"), 
                                   level = "PR", geo_format = "sf") %>% 
  mutate(name = if_else(name == "Quebec", "Québec", name)) %>% 
  select(name)


### Do geolocation and comparison ##############################################

## Get geolocation results for all Canada listings

Canada_provinces <- 
  property %>% 
  filter(country == "Canada") %>% 
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>% 
  st_transform(3857) %>% 
  st_nearest_feature(st_transform(provinces, 3857)) %>% 
  map_chr(~{provinces[.x,]$name})


## Create comparison table with AirDNA regions, geolocation and scraping

Canada_comparison <- 
  property %>% 
  filter(country == "Canada") %>% 
  select(property_ID, region:city) %>% 
  mutate(region_geo = Canada_provinces) %>% 
  left_join(select(scrape_results, property_ID, region_scrape = region, 
                   city_scrape = city)) %>% 
  mutate(region_scrape = case_when(
    region_scrape %in% c("Ab", "AL", "alberta", "ALberta", "Alberta, Canada") ~ 
      "Alberta",
    
    region_scrape %in% c("B C", "B.C", "B.C.", "bc", "Bc", "BC Canada",
                         "BC V0N2P0", "BC V1L", "BC,", "BC, North Saanich",
                         "BC/Canada", "British Colombia", "British Coloumbia", 
                         "British Columbia", "British Columbia ", 
                         "British Columbia (BC)", "British Columbia,",
                         "Colombie-Britannique", "Colombie Britannique", 
                         "Squamish BC") ~ 
      "British Columbia",
    
    region_scrape %in% c("Manitona", "Mb") ~
      "Manitoba",
    
    region_scrape %in% c("Nouveau-Brunswick") ~ 
      "New Brunswick",
    
    region_scrape %in% c("Newfoundland", "Nl") ~ 
      "Newfoundland and Labrador",
    
    region_scrape %in% c("Cape Breton", "Cape Breton    NS", "Lunenburg, NS", 
                         "Nouvelle-Écosse", "Nova scotia", "Nova Scotia", "Ns", 
                         "NS- 316", "NS,") ~ 
      "Nova Scotia",
    
    region_scrape %in% c("Cornwall", "Eganville", "En Ontario.", "on", "On", 
                         "ON", "ON / Prince Edward County", "ON,", "ON, Canada", 
                         "ont", "Ont", "ontario", "Ontario", "ONtario",
                         "ONTARIO", "Ontario,", "ONTARIO,", "ontario, canada", 
                         "Onterio", "Prince Edward County", 
                         "Prince Edward County ON", "TORONTO") ~ 
      "Ontario",
    
    region_scrape %in% c("PEI") ~
      "Prince Edward Island",
    
    region_scrape %in% c("Cantons-de-l'est", "charlevoix Québec", "Lachine",
                         "LaSalle,  Quebec", "Montreal", "Montreal, Québec",
                         "P.Québec", "PQ",
                         "Quebec", " Québec ", "-Quebec", "qc", "Qc", "QC ", 
                         "QC,", "Qu", "QuÃ©bec", "Que", "quebec", "QUEBEC", 
                         "Quebéc", "québec", "Québec", "QuÈbec", "Québec ", 
                         "Québec City", "Quebec, Canada", "Quecbec") ~ 
      "Québec", 
    
    region_scrape %in% c("Sk", "Saskatchwan") ~
      "Saskatchewan",
    
    region_scrape %in% c("Yukon Territory", "Yukon Territories") ~ 
      "Yukon",
    
    TRUE ~ region_scrape
  ))


## Identify cases with agreement or with disagreement

Canada_agree <-
  Canada_comparison %>% 
  filter((region == region_geo & region == region_scrape) |
           (is.na(region_scrape) & region == region_geo))

Canada_no_agree <- 
  Canada_comparison %>% 
  filter(!property_ID %in% Canada_agree$property_ID)


## Intersect Canada_no_agree with 5 km buffer around provincial boundaries

Canada_no_agree_valid <- 
  property %>% 
  filter(property_ID %in% Canada_no_agree$property_ID) %>% 
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%  
  st_transform(3857) %>% 
  st_intersection(st_buffer(st_transform(provinces, 3857), 5000)) %>% 
  st_drop_geometry() %>% 
  select(property_ID, name) %>% 
  inner_join(select(Canada_no_agree, property_ID, name = region_scrape))

Canada_no_agree <- 
  Canada_no_agree %>% 
  filter(!property_ID %in% Canada_no_agree_valid$property_ID)


## Accept geolocation result for remaining properties

Canada_finished <-
  Canada_agree %>% select(-region_geo, -region_scrape, -city_scrape) %>% 
  rbind(Canada_no_agree_valid %>% rename(region = name) %>% mutate(city = NA),
        Canada_no_agree %>% select(property_ID, region = region_geo, city))


### Bind results back into property table ######################################

property <-
  property %>% 
  filter(property_ID %in% Canada_finished$property_ID) %>% 
  select(-region, -city) %>% 
  inner_join(Canada_finished) %>% 
  select(property_ID:country, region, city, everything()) %>% 
  rbind(property %>% filter(!property_ID %in% Canada_finished$property_ID))


### Save temporary output ######################################################

save(property, file = "temp_10.Rdata")

rm(Canada_agree, Canada_comparison, Canada_finished, Canada_no_agree,
   Canada_no_agree_valid, provinces, Canada_provinces, province_names)


################################################################################
################################################################################



################################################################################
#### 10. FILL IN MISSING REGIONS IN USA ########################################

### Load geometry ##############################################################

state_names <- 
  c("Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", 
    "Connecticut", "Delaware", "District of Columbia", "Florida", "Georgia", 
    "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", 
    "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
    "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire",
    "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota",
    "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", 
    "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", 
    "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming")

states <- tigris::states(class = "sf") %>% 
  as_tibble() %>% 
  st_as_sf() %>% 
  st_transform(4326)


### Do geolocation and comparison ##############################################

## Get geolocation results for all US listings

US_states <- 
  property %>% 
  filter(country == "United States") %>% 
  split(1:(nrow(.)/1000))

US_states <-
  US_states %>% 
  pbapply::pblapply(function(x) {
    x %>% 
      st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>% 
      st_transform(3857) %>% 
      st_intersects(st_transform(states, 3857)) %>% 
      map_chr(~{if (length(.x) == 0) NA_character_ else states[.x,]$NAME})
    }, cl = 5) %>% 
  map2_df(US_states, ~{.y %>% mutate(NAME = .x)}) %>% 
  filter(!is.na(NAME))

US_nearest <- 
  property %>% 
  filter(country == "United States", 
         !property_ID %in% US_states$property_ID) %>% 
  split(1:(nrow(.)/10))

US_nearest <- 
  US_nearest %>% 
  pbapply::pblapply(function(x) {
    x %>% 
      st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%
      st_transform(3857) %>% 
      st_nearest_feature(st_transform(states, 3857)) %>% 
      map_chr(~{states[.x,]$NAME})
  }, cl = 5) %>% 
  map2_df(US_nearest, ~{.y %>% mutate(NAME = .x)})

US_states <- 
  bind_rows(US_states, US_nearest)


## Create comparison table with AirDNA regions, geolocation and scraping

US_comparison <- 
  property %>% 
  filter(country == "United States") %>% 
  select(property_ID, region:city) %>% 
  left_join(US_states) %>% 
  rename(region_geo = NAME) %>% 
  left_join(select(scrape_results, property_ID, region_scrape = region, 
                   city_scrape = city)) %>% 
  mutate(region_scrape = case_when(
    region_scrape %in% c("Al") ~ "Alabama",
    region_scrape %in% c("Ak") ~ "Alaska",
    region_scrape %in% c("Ar") ~ "Arkansas",
    region_scrape %in% c("AZ", "Az") ~ "Arizona",
    region_scrape %in% c("Ca", "ca", "Beverly Hills", "California ") ~ 
      "California",
    region_scrape %in% c("Co", "co") ~ "Colorado",
    region_scrape %in% c("Fl", "fl") ~ "Florida",
    region_scrape %in% c("Ga") ~ "Georgia",
    region_scrape %in% c("Hi") ~ "Hawaii",
    region_scrape %in% c("Id") ~ "Idaho",
    region_scrape %in% c("Il") ~ "Illinois",
    region_scrape %in% c("In") ~ "Indiana",
    region_scrape %in% c("Ks") ~ "Kansas",
    region_scrape %in% c("Ky") ~ "Kentucky",
    region_scrape %in% c("La") ~ "Louisiana",
    region_scrape %in% c("Ma") ~ "Massachusetts",
    region_scrape %in% c("Md") ~ "Maryland",
    region_scrape %in% c("Me") ~ "Maine",
    region_scrape %in% c("Allston, Ma") ~ "Massachusetts",
    region_scrape %in% c("Mi") ~ "Michigan",
    region_scrape %in% c("Ms") ~ "Mississippi",
    region_scrape %in% c("Mo") ~ "Missouri",
    region_scrape %in% c("Mt") ~ "Montana",
    region_scrape %in% c("Ne") ~ "Nebraska",
    region_scrape %in% c("Nv") ~ "Nevada",
    region_scrape %in% c("Nj") ~ "New Jersey",
    region_scrape %in% c("Ny", "ny", "NY ") ~ "New York",
    region_scrape %in% c("Oh") ~ "Ohio",
    region_scrape %in% c("Ok", "ok") ~ "Oklahoma",
    region_scrape %in% c("Or") ~ "Oregon",
    region_scrape %in% c("Pa") ~ "Pennsylvania",
    region_scrape %in% c("Ri") ~ "Rhode Island",
    region_scrape %in% c("Sc", "sc", "Beaufort County") ~ "South Carolina",
    region_scrape %in% c("Tn") ~ "Tennessee",
    region_scrape %in% c("Tx") ~ "Texas",
    region_scrape %in% c("Ut", "ut") ~ "Utah",
    region_scrape %in% c("Va") ~ "Virginia",
    region_scrape %in% c("Vt") ~ "Vermont",
    region_scrape %in% c("Wa") ~ "Washington",
    region_scrape %in% c("Wi") ~ "Wisconsin",
    TRUE ~ region_scrape
  ))


## Identify cases with agreement or with disagreement

US_agree <-
  US_comparison %>% 
  filter((region == region_geo & region == region_scrape) |
           (is.na(region_scrape) & region == region_geo))

US_no_agree <- 
  US_comparison %>% 
  filter(!property_ID %in% US_agree$property_ID)


## Intersect US_no_agree with 5 km buffer around state boundaries

US_no_agree_valid <- 
  property %>% 
  filter(property_ID %in% US_no_agree$property_ID) %>% 
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%  
  st_transform(3857) %>% 
  st_intersection(st_buffer(st_transform(states, 3857), 5000)) %>% 
  st_drop_geometry() %>% 
  select(property_ID, NAME) %>% 
  inner_join(select(US_no_agree, property_ID, NAME = region_scrape))

US_no_agree <- 
  US_no_agree %>% 
  filter(!property_ID %in% US_no_agree_valid$property_ID)


## Accept geolocation result for remaining properties

US_finished <-
  US_agree %>% select(property_ID, region, city) %>% 
  rbind(US_no_agree_valid %>% rename(region = NAME) %>% mutate(city = NA),
        US_no_agree %>% select(property_ID, region = region_geo, city))


### Bind results back into property table ######################################

property <-
  property %>% 
  filter(property_ID %in% US_finished$property_ID) %>% 
  select(-region, -city) %>% 
  inner_join(US_finished) %>% 
  select(property_ID:country, region, city, everything()) %>% 
  rbind(property %>% filter(!property_ID %in% US_finished$property_ID))


### Save temporary output ######################################################

save(property, file = "temp_11.Rdata")

rm(scrape_results, states, US_agree, US_comparison, US_finished, US_no_agree,
   US_no_agree_valid, US_states, state_names, US_nearest)


################################################################################
################################################################################



################################################################################
#### 11. FIX COUNTRIES WITH SPURIOUS REGION ENTRIES ############################

### Identify countries where fewer than 15% of listings have a region entry ####

region_whitelist <- 
  c("American Samoa", "Australia", "Belize", "Bolivia", "Brazil", "Canada", 
    "China", "Curaçao", "Egypt", "El Salvador", "Fiji", "France", "Germany", 
    "Ghana", "Guam", "Guatemala", "Honduras", "Indonesia", "Italy", "Japan", 
    "Jordan", "Kosovo", "Martinique", "Mauritius", "Mexico", "Nepal", 
    "New Zealand", "Nicaragua", "Northern Mariana Islands", "Panama", 
    "Philippines", "Portugal", "Puerto Rico", "Réunion", "Russia", 
    "Saint Barthélemy", "Saint Martin", "Senegal", "Sint Maarten", 
    "South Africa", "Spain", "Tanzania", "U.S. Virgin Islands", 
    "United Kingdom", "United States", "Venezuela")

regions_to_check <- 
  property %>% 
  group_by(country) %>% 
  summarize(region_pct = mean(!is.na(region))) %>% 
  filter(region_pct > 0, !country %in% region_whitelist)

listings_to_check <- 
  property %>% 
  filter(country %in% regions_to_check$country, !is.na(region))


### Remove region from these countries

listings_to_check_finished <-
  listings_to_check %>%
  mutate(region = NA_character_,
         city = NA_character_)


### Bind results back into property table ######################################

property <-
  property %>% 
  filter(property_ID %in% listings_to_check_finished$property_ID) %>% 
  select(-country, -region, -city) %>% 
  inner_join(listings_to_check_finished) %>% 
  select(property_ID:longitude, country, region, city, everything()) %>% 
  rbind(property %>% 
          filter(!property_ID %in% listings_to_check_finished$property_ID))


### Save temporary output ######################################################

save(property, file = "temp_12.Rdata")

rm(listings_to_check, listings_to_check_finished, regions_to_check,
   region_whitelist)


################################################################################
################################################################################



################################################################################
#### 12. DEAL WITH OTHER REGION ANOMALIES ######################################

### Harmonize Germany region names #############################################

Germany_fix <-
  property %>%
  filter(country == "Germany", region == "Hamburg") %>% 
  mutate(region = "Hamburg (Landmasse)")

property <-
  property %>% 
  filter(!property_ID %in% Germany_fix$property_ID) %>% 
  rbind(Germany_fix)


### Harmonize Russia region names ##############################################

Russia_fix <-
  property %>%
  filter(country == "Russia") %>% 
  mutate(region = case_when(
    region == "Dalnevostochnyj Federalnyj Okrug" ~ 
      "Far Eastern Federal District",
    region == "Sibirskij Federalnyj Okrug" ~ 
      "Siberian Federal District",
    region == "Privolzhskij Federalnyj Okrug" ~ 
      "Volga Federal District",
    region == "Severo Zapadnyj Federalnyj Okrug" ~ 
      "Northwestern Federal District",
    region == "Centralnyj Federal Okrug" ~ 
      "Central Federal District",
    region == "Severo Kavkazskij Federalnyj Okrug" ~ 
      "North Caucasian Federal District",
    region == "Uralskij Federalnyj Okrug" ~ 
      "Ural Federal District", 
    region == "Yuzhnyj Federalnyj Okrug" ~ 
      "Southern Federal District",
    TRUE ~ region
  ))

property <- 
  property %>% 
  filter(!property_ID %in% Russia_fix$property_ID) %>% 
  rbind(Russia_fix)


### Find countries with both valid and invalid region entries ##################

regions_with_many_countries <- 
  property %>% 
  filter(!is.na(region)) %>% 
  group_by(region) %>% 
  count(country) %>% 
  filter(n() >= 2) %>% 
  # Whitelist the regions that actually exist in two countries
  filter(!region %in% 
           c("Amazonas", "Distrito Federal", "Granada", "La Paz", "Leon"))


## Find countries with more or fewer entries of a region to identify problems

winners <- 
  regions_with_many_countries %>% 
  filter(n == max(n)) %>% 
  ungroup()

losers <- 
  regions_with_many_countries %>% 
  filter(n != max(n)) %>% 
  ungroup()

regions_many_property <- 
  map2_df(losers$region, losers$country, ~{
    property %>% filter(region == .x, country == .y)
  }) %>% 
  arrange(property_ID)


## Trust geolocation and purge region and city

regions_many_property <- 
  regions_many_property %>% 
  mutate(region = NA_character_,
         city = NA_character_)


### Bind results back into property table ######################################

property <-
  property %>% 
  filter(!property_ID %in% regions_many_property$property_ID) %>% 
  rbind(regions_many_property)


### Save temporary output ######################################################

save(property, file = "temp_13.Rdata")

rm(Germany_fix, Russia_fix, regions_with_many_countries, regions_many_property,
   winners, losers)


################################################################################
################################################################################



################################################################################
#### 13. FILL IN MISSING REGIONS USING CONVEX HULLS ############################

### Load existing region hulls

load("data/region_hulls.Rdata")


### Update convex hulls for all regions ########################################

region_hulls_2 <-
  property %>% 
  filter(!is.na(country), !is.na(region), 
         # Exclude Canada and US because they are handled separately
         country != "Canada", country != "United States") %>% 
  mutate(country_region = paste(country, region, sep = ", ")) %>% 
  filter(country_region %in% 
           (mutate(region_hulls, 
                   country_region = paste(country, region, sep = ", "))
            )$country_region) %>% 
  group_by(country, region) %>% 
  group_split() %>% 
  pbapply::pblapply(function(x) {
    x %>% 
      st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>% 
      st_transform(3857) %>% 
      group_by(country, region) %>% 
      summarize() %>% 
      mutate(geometry = st_union(geometry, filter(region_hulls, 
                       country == .$country, region == .$region))) %>% 
      st_convex_hull()
  }, cl = 5)

region_hulls_2 <- 
  region_hulls_2 %>%
  data.table::rbindlist() %>% 
  as_tibble() %>% 
  st_as_sf() %>% 
  mutate(country_region = paste(country, region, sep = ", "))

region_hulls <-
  region_hulls %>% 
  mutate(country_region = paste(country, region, sep = ", ")) %>%
  filter(!country_region %in% region_hulls_2$country_region) %>% 
  rbind(region_hulls_2) %>% 
  select(-country_region)


### Intersect listings without regions with the hull of their country ##########

country_groups <- 
  property %>% 
  filter(!is.na(country), is.na(region)) %>% 
  group_split(country)
  
region_intersects <- 
  country_groups %>% 
  pbapply::pblapply(function(x) {
    
    country_hulls <- region_hulls %>% filter(country == x[1,]$country)
    
    intersects <- 
      x %>% 
      st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>% 
      st_transform(3857) %>% 
      st_intersects(country_hulls)
    
    # For listings only intersecting one hull, assign them to that region
    map_chr(intersects, ~{
      if (length(.x) == 1) country_hulls[.x,]$region else NA_character_
    })
  }, cl = 5) %>% 
  map2_df(country_groups, ~{mutate(.y, region = .x)})


### Bind results back into property table ######################################

property <-
  property %>% 
  filter(!property_ID %in% region_intersects$property_ID) %>% 
  bind_rows(region_intersects)


### Save updated region hulls and temporary output #############################

save(region_hulls, file = "data/region_hulls.Rdata")

save(property, file = "temp_14.Rdata")

rm(country_groups, region_hulls, region_hulls_2, region_intersects)


################################################################################
################################################################################



################################################################################
#### 14. FINALIZE PROPERTY TABLE AND LOCATION TABLE ############################

### Check property file for duplicates #########################################

if ({
  property %>% 
    count(property_ID) %>% 
    filter(n > 1) %>% 
    nrow() > 0}) {stop("Need to check additional location_table duplicates")}


### Merge property into master files ###########################################

property_new <- property

load("temp_2.Rdata")

property <- 
  bind_rows(property_fixed, property_new)


### Produce final location_table ###############################################

location_table <- 
  property %>% 
  select(property_ID, latitude, longitude, country, region, city)


### Save output to disk and delete temporary files #############################

save(property, file = paste0("output/property/property_", year_month, ".Rdata"))

save(location_table, 
     file = paste0("output/property/location_table_", year_month, ".Rdata"))

file.remove("temp_1.Rdata", "temp_2.Rdata", "temp_3.Rdata", "temp_4.Rdata", 
            "temp_5.Rdata", "temp_6.Rdata", "temp_7.Rdata", "temp_8.Rdata",
            "temp_9.Rdata", "temp_10.Rdata", "temp_11.Rdata", "temp_12.Rdata",
            "temp_13.Rdata", "temp_14.Rdata")

rm(location_table, property_fixed, property_new)


################################################################################
################################################################################



################################################################################
#### 15. UPLOAD PROPERTY FILE ##################################################

### Prepare queries ############################################################

property_drop <- 'DROP TABLE public.property;'

property_create <- 'CREATE TABLE public.property
(
    "property_ID" text COLLATE pg_catalog."default" NOT NULL,
    "host_ID" text COLLATE pg_catalog."default",
    listing_title text COLLATE pg_catalog."default",
    property_type text COLLATE pg_catalog."default",
    listing_type text COLLATE pg_catalog."default",
    created date,
    scraped date,
    housing boolean,
    latitude real,
    longitude real,
    country text COLLATE pg_catalog."default",
    region text COLLATE pg_catalog."default",
    city text COLLATE pg_catalog."default",
    neighbourhood text COLLATE pg_catalog."default",
    metro_area text COLLATE pg_catalog."default",
    currency text COLLATE pg_catalog."default",
    bedrooms real,
    bathrooms real,
    max_guests real,
    response_rate real,
    superhost boolean,
    premier_partner boolean,
    cancellation text COLLATE pg_catalog."default",
    security_deposit real,
    cleaning_fee real,
    extra_people_fee real,
    check_in_time text COLLATE pg_catalog."default",
    check_out_time text COLLATE pg_catalog."default",
    minimum_stay real,
    num_reviews real,
    num_photos real,
    instant_book boolean,
    rating real,
    ab_property real,
    ab_host real,
    ha_property text COLLATE pg_catalog."default",
    ha_host text COLLATE pg_catalog."default",
    CONSTRAINT "property_pkey" PRIMARY KEY ("property_ID")
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;'

property_perm_dw <- 'GRANT ALL ON TABLE public.property TO dwachsmuth;'
property_perm_ro <-  'GRANT SELECT ON TABLE public.property TO read_only;'

property_index_host_ID <-
  'CREATE INDEX "property_index_host_ID"
  ON public.property USING btree
  ("host_ID" COLLATE pg_catalog."default")
  TABLESPACE pg_default;'

property_index_country <-
  'CREATE INDEX property_index_country
  ON public.property USING btree
  (country COLLATE pg_catalog."default")
  TABLESPACE pg_default;'

property_index_region <- 
  'CREATE INDEX property_index_region
  ON public.property USING btree
  (region COLLATE pg_catalog."default")
  TABLESPACE pg_default;'

property_index_city <-
  'CREATE INDEX property_index_city
  ON public.property USING btree
  (city COLLATE pg_catalog."default")
  TABLESPACE pg_default;'


### Execute queries to remove old table and create new one #####################

con <- RPostgres::dbConnect(RPostgres::Postgres(), dbname = "airdna")

dbExecute(con, property_drop)
dbExecute(con, property_create)
dbExecute(con, property_perm_dw)
dbExecute(con, property_perm_ro)
dbExecute(con, property_index_host_ID)
dbExecute(con, property_index_country)
dbExecute(con, property_index_region)
dbExecute(con, property_index_city)

rm(property_drop, property_create, property_perm_dw, property_perm_ro, 
   property_index_host_ID, property_index_country, property_index_region,
   property_index_city)


### Upload table ###############################################################

Sys.time()
RPostgres::dbWriteTable(con, "property", property, append = TRUE)
Sys.time()


################################################################################
################################################################################



################################################################################
#### 16. PREPARE DAILY FILE 1 ##################################################

### Import daily file ##########################################################

load("data/daily_length.Rdata")

daily <- read_csv(paste0("data/daily_", year_month, ".csv"), 
                  col_names = c("Property ID", "Date", "Status", "Booked Date",
                                "Price (USD)", "Price (Native)",
                                "Currency Native", "Reservation ID", 
                                "Airbnb Property ID", "HomeAway Property ID"),
                  col_types = "cDcDddcddc", skip = 1, 
                  n_max = floor(daily_length / 3)) %>% 
  select(`Property ID`, Date, Status, `Booked Date`, `Price (USD)`,
         `Reservation ID`)


### Prepare ML table ###########################################################

ML_1 <- 
  daily %>% 
  set_names("property_ID", "date", "status", "booked_date", "price", 
            "res_ID") %>% 
  left_join(select(property, property_ID, host_ID, listing_type)) %>% 
  count(host_ID, date, listing_type)

save(ML_1, file = "temp_ML_1.Rdata")

rm(ML_1)


### Compress daily file, join to property file, and save output ################

output_1 <- strr_compress(daily, cores = 5)

daily <- 
  output_1[[1]] %>% 
  left_join(select(property, property_ID, host_ID, listing_type:housing,
                   country:city))

save(daily, file = paste0("output/", substr(year_month, 1, 4), 
                          "/daily_", year_month, "_1.Rdata"))
write_csv(output_1[[2]], paste0("output/", substr(year_month, 1, 4), "/error_", 
                                year_month, "_1.csv")) 
write_csv(output_1[[3]], paste0("output/", substr(year_month, 1, 4), 
                                "/missing_rows_", year_month, "_1.csv"))


### Add to no_property_ID file #################################################

load(paste0("output/2019/no_PID_", last_month, ".Rdata"))

no_property_ID <- 
  daily %>% 
  filter(!(property_ID %in% property$property_ID)) %>% 
  rbind(no_property_ID)

save(no_property_ID, file = "temp_no_PID.Rdata")

rm(output_1)


################################################################################
################################################################################



################################################################################
#### 17. PREPARE DAILY FILE 2 ##################################################

### Import daily file ##########################################################

load("data/daily_length.Rdata")

daily <- read_csv(paste0("data/daily_", year_month, ".csv"), 
                  col_names = c("Property ID", "Date", "Status", "Booked Date",
                                "Price (USD)", "Price (Native)",
                                "Currency Native", "Reservation ID", 
                                "Airbnb Property ID", "HomeAway Property ID"),
                  col_types = "cDcDddcddc", 
                  skip = 1 + floor(daily_length / 3),
                  n_max = floor(daily_length / 3)) %>% 
  select(`Property ID`, Date, Status, `Booked Date`, `Price (USD)`,
         `Reservation ID`)


### Prepare ML table ###########################################################

ML_2 <- 
  daily %>% 
  set_names("property_ID", "date", "status", "booked_date", "price", 
            "res_ID") %>% 
  left_join(select(property, property_ID, host_ID, listing_type)) %>% 
  count(host_ID, date, listing_type)

save(ML_2, file = "temp_ML_2.Rdata")

rm(ML_2)


### Compress daily file, join to property file, and save output ################

output_2 <- strr_compress(daily, cores = 5)

daily <- 
  output_2[[1]] %>% 
  left_join(select(property, property_ID, host_ID, listing_type:housing,
                   country:city))

save(daily, file = paste0("output/", substr(year_month, 1, 4), 
                          "/daily_", year_month, "_2.Rdata"))
write_csv(output_2[[2]], paste0("output/", substr(year_month, 1, 4), "/error_", 
                                year_month, "_2.csv")) 
write_csv(output_2[[3]], paste0("output/", substr(year_month, 1, 4), 
                                "/missing_rows_", year_month, "_2.csv"))


### Add to no_property_ID file #################################################

load("temp_no_PID.Rdata")

no_property_ID <- 
  daily %>% 
  filter(!(property_ID %in% property$property_ID)) %>% 
  rbind(no_property_ID)

save(no_property_ID, file = "temp_no_PID.Rdata")

rm(output_2)


################################################################################
################################################################################



################################################################################
#### 18. PREPARE DAILY FILE 3 ##################################################

### Import daily file ##########################################################

load("data/daily_length.Rdata")

daily <- read_csv(paste0("data/daily_", year_month, ".csv"), 
                  col_names = c("Property ID", "Date", "Status", "Booked Date",
                                "Price (USD)", "Price (Native)",
                                "Currency Native", "Reservation ID", 
                                "Airbnb Property ID", "HomeAway Property ID"),
                  col_types = "cDcDddcddc", 
                  skip = 1 + 2 * floor(daily_length / 3)) %>% 
  select(`Property ID`, Date, Status, `Booked Date`, `Price (USD)`,
         `Reservation ID`)


### Prepare ML table ###########################################################

ML_3 <- 
  daily %>% 
  set_names("property_ID", "date", "status", "booked_date", "price", 
            "res_ID") %>% 
  left_join(select(property, property_ID, host_ID, listing_type)) %>% 
  count(host_ID, date, listing_type)

save(ML_3, file = "temp_ML_3.Rdata")

rm(ML_3)


### Compress daily file, join to property file, and save output ################

output_3 <- strr_compress(daily, cores = 6)

daily <- 
  output_3[[1]] %>% 
  left_join(select(property, property_ID, host_ID, listing_type:housing,
                   country:city))

save(daily, file = paste0("output/", substr(year_month, 1, 4), 
                          "/daily_", year_month, "_3.Rdata"))
write_csv(output_3[[2]], paste0("output/", substr(year_month, 1, 4), "/error_", 
                                year_month, "_3.csv")) 
write_csv(output_3[[3]], paste0("output/", substr(year_month, 1, 4), 
                                "/missing_rows_", year_month, "_3.csv"))


### Add to no_property_ID file and export file #################################

no_property_ID <- 
  daily %>% 
  filter(!(property_ID %in% property$property_ID)) %>% 
  rbind(no_property_ID)

save(no_property_ID, file = paste0("output/", substr(year_month, 1, 4), 
                                   "/no_PID_", year_month, ".Rdata"))

file.remove("temp_no_PID.Rdata")

rm(output_3, no_property_ID, daily)


################################################################################
################################################################################



################################################################################
#### 19. UPLOAD DAILY FILES ####################################################

### Upload file 1 ##############################################################

load(paste0("output/", substr(year_month, 1, 4), "/daily_", year_month,
            "_1.Rdata"))

con <- RPostgres::dbConnect(RPostgres::Postgres(), dbname = "airdna")

Sys.time()
daily %>% 
  filter(property_ID %in% property$property_ID) %>% 
  RPostgres::dbWriteTable(con, "daily", ., append = TRUE)
Sys.time()

daily_all <- tbl(con, "daily")

daily_all %>% 
  filter(property_ID == !! daily[1,]$property_ID,
         start_date == !! daily[1,]$start_date) %>% 
  collect() %>% 
  nrow()


### Upload file 2 ##############################################################

load(paste0("output/", substr(year_month, 1, 4), "/daily_", year_month,
            "_2.Rdata"))

con <- RPostgres::dbConnect(RPostgres::Postgres(), dbname = "airdna")

Sys.time()
daily %>% 
  filter(property_ID %in% property$property_ID) %>% 
  RPostgres::dbWriteTable(con, "daily", ., append = TRUE)
Sys.time()

daily_all <- tbl(con, "daily")

daily_all %>% 
  filter(property_ID == !! daily[1,]$property_ID,
         start_date == !! daily[1,]$start_date) %>% 
  collect() %>% 
  nrow()


### Upload file 3 ##############################################################

load(paste0("output/", substr(year_month, 1, 4), "/daily_", year_month,
            "_3.Rdata"))

con <- RPostgres::dbConnect(RPostgres::Postgres(), dbname = "airdna")

Sys.time()
daily %>% 
  filter(property_ID %in% property$property_ID) %>% 
  RPostgres::dbWriteTable(con, "daily", ., append = TRUE)
Sys.time()

daily_all <- tbl(con, "daily")

daily_all %>% 
  filter(property_ID == !! daily[1,]$property_ID,
         start_date == !! daily[1,]$start_date) %>% 
  collect() %>% 
  nrow()

rm(con, daily_all)


################################################################################
################################################################################



################################################################################
#### 20. PREPARE AND UPLOAD ML TABLE ###########################################

### Load files #################################################################

load("temp_ML_1.Rdata")
load("temp_ML_2.Rdata")
load("temp_ML_3.Rdata")

### Bind and compress the three separate tables ################################

ML <- 
  bind_rows(ungroup(ML_1), ungroup(ML_2), ungroup(ML_3)) %>% 
  group_by(host_ID, date, listing_type) %>% 
  summarize(count = sum(n)) %>% 
  ungroup()

rm(ML_1, ML_2, ML_3)

ML <- 
  ML %>% 
  ungroup() %>% 
  strr_compress(cores = 6)


### Save final ML table to disk ################################################

save(ML, file = paste0("output/ML/ML_", year_month, ".Rdata"))

file.remove("temp_ML_1.Rdata")
file.remove("temp_ML_2.Rdata")
file.remove("temp_ML_3.Rdata")


### Produce updated no_ML file #################################################

load(paste0("output/ML/no_ML_", last_month, ".Rdata"))

no_ML <- 
  ML %>% 
  filter(!(host_ID %in% property$host_ID)) %>% 
  rbind(no_ML)

save(no_ML, file = paste0("output/ML/no_ML_", year_month, ".Rdata"))

rm(no_ML)


### Upload ML table ############################################################

upgo_connect(property = FALSE, daily = FALSE, ML = FALSE)

Sys.time()
ML %>% 
  filter(host_ID %in% property$host_ID) %>% 
  RPostgres::dbWriteTable(con, "ML", ., append = TRUE)
Sys.time()


upgo_disconnect()






################################################################################
################################################################################
################################################################################


