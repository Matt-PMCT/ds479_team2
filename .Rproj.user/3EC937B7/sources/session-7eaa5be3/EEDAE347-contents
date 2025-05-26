# Google Trends Dataset Getter
# This script uses gtrendsR to retrieve google interest in the districts
# edit: turns out gtrendsR is broken :(

# 0.  Install / load libraries
required_pkgs <- c(
  "gtrendsR", "tidyverse", "fs", "lubridate"
)
# Check for missing packages and install them if necessary
missing_pkgs <- setdiff(required_pkgs, rownames(installed.packages()))
if (length(missing_pkgs)) 
  install.packages(missing_pkgs, repos = "https://cloud.r-project.org")
# Ensure they load in a reproducible order, hide the output
invisible(lapply(required_pkgs, library, character.only = TRUE))
# 1. ensure input CSV is present
csv_file <- "./datasets/geocode_cache.csv"
if (!file.exists(csv_file)) {
  stop(sprintf("Error: CSV file '%s' not found in the working directory.", csv_file))
}
# 0. ensure output directory exists
output_dir <- "./datasets/google_trends_data"
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# 1. check for input CSV
csv_file <- "./datasets/geocode_cache.csv"
if (!file.exists(csv_file)) {
  stop(sprintf("Error: CSV file '%s' not found in working directory.", csv_file))
}

# 2. read and clean district list
districts <- read_csv(csv_file, show_col_types = FALSE) %>%
  pull(district) %>%
  unique() %>%
  discard(is.na) %>%
  discard(~ str_trim(.x) == "")

# 3. safe gtrends wrapper with retries
safe_gtrends <- function(keyword,
                         geo         = "",
                         time_window = "2005-01-01 2024-12-31",
                         max_retries = 5) {
  for (i in seq_len(max_retries)) {
    Sys.sleep(runif(1, 2, 5))
    result <- tryCatch(
      gtrends(keyword = keyword, geo = geo, time = time_window, gprop = "web"),
      error = function(e) e
    )
    if (!inherits(result, "error")) {
      return(result)
    }
    msg <- result$message
    if (grepl("429", msg) && i < max_retries) {
      wait <- i * 10
      message(sprintf("Rate limited for '%s' (retry %d/%d). Waiting %d seconds...", 
                      keyword, i, max_retries, wait))
      Sys.sleep(wait)
    } else {
      message(sprintf("gtrends failed for '%s': %s", keyword, msg))
      return(NULL)
    }
  }
}

# 4. loop over each district
for (d in districts) {
  # build safe filename
  fname <- paste0(str_replace_all(d, "[^[:alnum:]_]", "_"), ".csv")
  fp <- file.path(output_dir, fname)
  
  # skip if already done
  if (file.exists(fp)) {
    message(sprintf("Skipping '%s': output already exists.", d))
    next
  }
  
  # prepare the search term
  if (str_detect(d, regex("district$", ignore_case = TRUE))) {
    search_term <- str_replace(d, regex("district", ignore_case = TRUE), "New Zealand")
  } else if (str_detect(d, regex("city", ignore_case = TRUE))) {
    search_term <- paste(d, "New Zealand")
  } else if (str_detect(d, regex("Auckland", ignore_case = TRUE))) {
    search_term <- paste(d, "New Zealand")
  } else {
    search_term <- d
  }
  
  # call Google Trends
  tr <- safe_gtrends(search_term)
  if (is.null(tr)) next
  
  # extract monthly data and rename hits
  df <- tr$interest_over_time %>%
    mutate(month = floor_date(date, "month")) %>%
    transmute(
      month,
      search_interest = as.numeric(hits)
    )
  
  # save per‐district CSV
  write_csv(df, fp)
  message(sprintf("Saved trends for '%s' to %s", d, fp))
}
