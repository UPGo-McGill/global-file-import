### PROPERTY FILE IMPORTER ##############

library(tidyverse)

spec_csv("data/property_2019-05.csv")

property <- read_csv("data/property_2019-05.csv",
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


error <- 
  problems(property) %>% 
  filter(expected != "56 columns") %>% 
  pull(row) %>% 
  {property[.,]}

property <- 
  problems(property) %>% 
  filter(expected != "56 columns") %>% 
  pull(row) %>% 
  {property[-.,]}



error <- 
  property %>% 
  filter(is.na(property_ID)) %>% 
  rbind(error)

property <- 
  property %>% 
  filter(!is.na(property_ID))


missing_geography <- 
  property %>% 
  filter(is.na(latitude) | is.na(longitude))

property <- 
  property %>% 
  filter(!is.na(latitude), !is.na(longitude))


property <- 
  property %>% 
  mutate(listing_title = str_replace_all(
    listing_title, c('\n' = "", '\r' = "", '\"' = "", "\'" = "")))

