# This file loads and caches geocoding results, then fetches archival weather data.
# It only needs to be run once if the weather data does not yet exists
# It also will take hours to run since the free weather API only allows so many requests.

# 0) Install missing packages if needed, then load libraries
required_packages <- c(
  "readr",         # CSV I/O
  "dplyr",         # data manipulation
  "tidyr",         # data tidying
  "tidygeocoder",  # geocoding
  "httr",          # HTTP requests
  "jsonlite",      # JSON parsing
  "prettyunits"    # dependency of tidygeocoder
)

installed <- rownames(installed.packages())
for(pkg in required_packages){
  if(!(pkg %in% installed)){
    install.packages(pkg,
                     repos       = "https://cloud.r-project.org",
                     dependencies = TRUE)
  }
}

library(readr)
library(dplyr)
library(tidyr)
library(tidygeocoder)
library(httr)
library(jsonlite)
library(prettyunits)

# 1) Read dwelling consents CSV and extract district list
df <- read_csv(
  "./datasets/building_consents/New_dwellings_consented_per_1000_residents_by_territorial_authority_(Annual).csv"
)

districts <- df %>%
  pull(Series_title_1) %>%
  unique() %>%
  setdiff("New Zealand")

# 2) Load or initialize geocode cache
cache_file <- "./datasets/geocode_cache.csv"
if (file.exists(cache_file)) {
  geocode_cache <- read_csv(cache_file,
                            col_types = cols(
                              district  = col_character(),
                              latitude  = col_double(),
                              longitude = col_double()
                            ))
} else {
  geocode_cache <- tibble(
    district  = character(),
    latitude  = double(),
    longitude = double()
  )
}

# 3) Identify new districts to geocode
to_geocode <- tibble(district = districts) %>%
  anti_join(geocode_cache, by = "district")

# 4) Geocode new districts
if (nrow(to_geocode) > 0) {
  new_geo <- to_geocode %>%
    geocode(
      address      = district,
      method       = 'osm',
      lat          = latitude,
      long         = longitude,
      full_results = FALSE
    ) %>%
    select(district, latitude, longitude)
  
  missing_geo <- new_geo %>% filter(is.na(latitude) | is.na(longitude))
  if (nrow(missing_geo) > 0) {
    warning("Failed to geocode: ",
            paste(missing_geo$district, collapse = ", "))
  }
  
  geocode_cache <- bind_rows(geocode_cache, new_geo)
  write_csv(geocode_cache, cache_file)
}

# 5) Prepare for archive API requests
geo_df     <- filter(geocode_cache, district %in% districts)
start_date <- as.Date("2000-01-01")
end_date   <- as.Date("2025-01-01")
timezone   <- "Pacific/Auckland"
base_url   <- "https://archive-api.open-meteo.com/v1/archive"

daily_vars <- c(
  "temperature_2m_max",
  "temperature_2m_min",
  "temperature_2m_mean",
  "weather_code",
  "sunshine_duration",
  "precipitation_sum",
  "snowfall_sum"
) %>% paste(collapse = ",")

# 6) Loop over each district, fetch archival data, and save to CSV
for (i in seq_len(nrow(geo_df))) {
  dist_name <- geo_df$district[i]
  lat       <- geo_df$latitude[i]
  lon       <- geo_df$longitude[i]
  if (is.na(lat) || is.na(lon)) next  # skip invalid coords
  
  # Build safe output filename
  safe_name <- gsub("[^A-Za-z0-9_\\-]", "_", dist_name)
  out_file  <- sprintf("./datasets/weather/weather-%s.csv", safe_name)
  
  # Skip if we already have the file
  if (file.exists(out_file)) {
    message("Skipping ", dist_name, ": already have ", out_file)
    next
  }
  
  # Construct archive API URL
  url <- modify_url(
    url = base_url,
    query = list(
      latitude    = lat,
      longitude   = lon,
      daily       = daily_vars,
      start_date  = as.character(start_date),
      end_date    = as.character(end_date),
      timezone    = timezone
    )
  )
  
  
  # I am asking for a lot from the free API here, so I need to try to rate limit
  # and handle 429 errors. 
  resp <- GET(url)
  if (status_code(resp) == 429) {
    msg1 <- content(resp, as = "text", encoding = "UTF-8")
    # decide sleep duration based on server message
    if (grepl("Hourly API request limit exceeded", msg1, fixed = TRUE)) {
      message("Hourly limit exceeded for ", dist_name, 
              "—waiting 1 hour then retrying...")
      Sys.sleep(3600)
    } else {
      message("Rate limit hit for ", dist_name, "—server says: ", msg1, 
              "—waiting 60 s then retrying...")
      Sys.sleep(60)
    }
    # retry once
    resp <- GET(url)
    if (status_code(resp) == 429) {
      msg2 <- content(resp, as = "text", encoding = "UTF-8")
      stop(sprintf(
        "Still getting 429 for %s after retry—server message:\n%s",
        dist_name, msg2
      ))
    }
  }
  
  if (status_code(resp) != 200) {
    warning("HTTP ", status_code(resp),
            " fetching archive for ", dist_name)
    next
  }
  
  # Parse JSON and write CSV
  weather_json <- content(resp, as = "text", encoding = "UTF-8")
  weather_data <- fromJSON(weather_json, flatten = TRUE)
  daily_df     <- as_tibble(weather_data$daily)
  write_csv(daily_df, out_file)
  message("Saved archive data for ", dist_name, " → ", out_file)
}

