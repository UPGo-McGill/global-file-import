#### UNIVERSAL IMPORT FUNCTION #################################################

library(tidyverse)
library(data.table)
library(lubridate)
library(parallel)
library(pbapply)

read_second <- function(path, ...) {
  read_csv(path,
           col_names = c("property_ID", "date", "status", "booked_date",
                         "price", "currency", "res_id"),
           col_types = cols_only(
             property_ID = col_character(),
             date = col_date(format = ""),
             status = col_character(),
             booked_date = col_date(format = ""),
             price = col_integer(),
             currency = col_character(),
             res_id = col_integer()
             ),
           ...)
}



prepare <- function(daily) {
  
  ## Find rows with readr errors and add to error file
  
  error <- 
    problems(daily) %>% 
    filter(expected != "10 columns", expected != "no trailing characters") %>% 
    pull(row) %>% 
    daily[.,]
  
  error_vector <- 
    problems(daily) %>% 
    filter(expected != "10 columns", expected != "no trailing characters") %>% 
    pull(row)
  
  if (length(error_vector) > 0) {daily <- daily[-error_vector,]}
  
  ## Find rows with missing property_ID, date or status
  
  error <- 
    daily %>% 
    filter(is.na(property_ID) | is.na(date) | is.na(status)) %>% 
    rbind(error)
  
  daily <- 
    daily %>% 
    filter(!is.na(property_ID), !is.na(date), !is.na(status))
  
  ## Check status
  
  error <- 
    daily %>% 
    filter(!(status %in% c("A", "U", "B", "R"))) %>% 
    rbind(error)
  
  daily <- 
    daily %>% 
    filter(status %in% c("A", "U", "B", "R"))
  
  ## Remove duplicate listing entries by price, but don't add to error file
  
  daily <- 
    daily %>% 
    filter(!is.na(price))
  
  ## Find rows with bad currency entries
  
  currencies <- c("USD", "EUR", "BRL", "DKK", "NZD", "AED", "CAD", "GBP", "PLN",
                  "CHF", "AUD", "CNY", "HKD", "THB", "RUB", "JPY", "ILS", "CZK",
                  "SEK", "HUF", "ZAR", "SGD", "TRY", "MXN", "PHP", "KRW", "ARS",
                  "NOK", "CLP", "INR", "IDR", "UAH", "COP", "TWD", "BGN", "MYR",
                  "HRK", "VND", "RON", "PEN", "SAR", "MAD", "CRC", "UYU", "XPF",
                  NA)
  
  error <- 
    daily %>% 
    filter(!(currency %in% currencies)) %>% 
    rbind(error)
  
  daily <- 
    daily %>% 
    filter(currency %in% currencies)
  
  ## Find missing rows
  
  setDT(daily)
  missing_rows <-
    daily[, list(count = .N, 
                 full_count = as.integer(max(date) - min(date) + 1)), 
          by = property_ID]
  missing_rows[, dif := full_count - count]
  missing_rows <- missing_rows[dif != 0]
  
  ## Split into list
  
  daily <- 
    daily %>% 
    as_tibble() %>% 
    mutate(month = month(date), year = year(date))
  
  daily_list <- split(daily, daily$property_ID)
  
  daily_list <- map(1:100, function(i) {
    rbindlist(
      daily_list[(floor(length(daily_list) * (i - 1) / 100) + 
                    1):floor(length(daily_list) * i / 100)])
  })
  
  ## Output
  
  return(list(daily_list, error, missing_rows))
}


splitter <- function(daily) {
  
  daily <- 
    daily %>% 
    group_by(property_ID, status, booked_date, price, currency, res_id, month, 
             year) %>% 
    summarize(dates = list(date)) %>%  
    ungroup()
  
  single_date <- 
    daily %>% 
    filter(map(dates, length) == 1) %>% 
    mutate(start_date = as.Date(map_dbl(dates, ~{.x}), origin = "1970-01-01"),
           end_date = as.Date(map_dbl(dates, ~{.x}), origin = "1970-01-01")) %>% 
    select(property_ID, start_date, end_date, status, booked_date, price,
           currency, res_id)
  
  one_length <- 
    daily %>% 
    filter(map(dates, length) != 1,
           map(dates, ~{length(.x) - length(min(.x):max(.x))}) == 0) %>% 
    mutate(start_date = as.Date(map_dbl(dates, min), origin = "1970-01-01"),
           end_date = as.Date(map_dbl(dates, max), origin = "1970-01-01")) %>% 
    select(property_ID, start_date, end_date, status, booked_date, price,
           currency, res_id)
  
  remainder <- 
    daily %>% 
    filter(map(dates, length) != 1,
           map(dates, ~{length(.x) - length(min(.x):max(.x))}) != 0) %>% 
    mutate(date_range = map(dates, ~{
      tibble(start_date = .x[which(diff(c(0, .x)) > 1)],
             end_date = .x[which(diff(c(.x, 30000)) > 1)])
    })) %>% 
    unnest(date_range) %>% 
    select(property_ID, start_date, end_date, status, booked_date, price,
           currency, res_id)
  
  bind_rows(single_date, one_length, remainder) %>% 
    arrange(property_ID, start_date)
  
}


compress <- function(daily_list) {
  daily_list %>%
  pbapply::pblapply(splitter, cl = 5) %>%
  do.call(rbind, .)
}





