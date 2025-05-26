# 0. prepare

required_pkgs <- c(
  "httr","jsonlite","tidyverse"
)
# Check for missing packages and install them if necessary
missing_pkgs <- setdiff(required_pkgs, rownames(installed.packages()))
if (length(missing_pkgs)) 
  install.packages(missing_pkgs, repos = "https://cloud.r-project.org")
# Ensure they load in a reproducible order, hide the output
invisible(lapply(required_pkgs, library, character.only = TRUE))

tz <- 420
input_csv  <- "datasets/geocode_cache.csv"
output_dir <- "datasets/google_trends_data"
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

# 1. sanity check & load districts
if (!file.exists(input_csv)) {
  stop("Error: CSV file '", input_csv, "' not found.")
}
districts <- read_csv(input_csv, show_col_types = FALSE) %>%
  pull(district) %>% 
  unique() %>%
  discard(is.na) %>%
  discard(~ str_trim(.x) == "")

# 2. helper: fetch widget token & request JSON
get_widget <- function(term) {
  # pause for 20 seconds to avoid rate limiting
  Sys.sleep(20 + runif(1, 0, 5))
  req_explore <- list(
    comparisonItem = list(
      list(keyword = term, geo = "", time = "2005-01-01 2024-12-31")
    ),
    category = 0,
    property = ""
  )
  res <- GET(
    "https://trends.google.com/trends/api/explore",
    query = list(req = toJSON(req_explore, auto_unbox = TRUE), tz = tz),
    user_agent("Mozilla/5.0")
  )
  stop_for_status(res)
  
  txt  <- content(res, "text", encoding = "UTF-8")
  json <- fromJSON(sub("^[^\\{]+", "", txt))
  
  print(txt)
  # loop until we find a list element with id == "TIMESERIES"
  widget <- NULL
  for (w in json$widgets) {
    if (is.list(w) && !is.null(w$id) && w$id == "TIMESERIES") {
      widget <- w
      break
    }
  }
  if (is.null(widget)) {
    stop(sprintf("TIMESERIES widget not found for '%s'", term))
  }
  list(token = widget$token, request = widget$request)
}


# 3. helper: download & parse CSV
download_csv <- function(widget){
  res <- GET(
    "https://trends.google.com/trends/api/widgetdata/multiline/csv",
    query = list(
      req   = toJSON(widget$request, auto_unbox = TRUE),
      token = widget$token,
      tz    = tz
    ),
    user_agent("Mozilla/5.0")
  )
  stop_for_status(res)
  txt   <- content(res, "text", encoding = "UTF-8")
  lines <- str_split(txt, "\n")[[1]]
  start <- which(str_detect(lines, "^Date,"))[1]
  csv   <- paste(lines[start:length(lines)], collapse = "\n")
  read_csv(csv)
}

# 4. main loop
# right before your loop, inspect the districts object:
message("There are ", length(districts), " districts to process.")
str(districts)

# now the loop with extra debug printing
for (d in districts) {
  message("----")
  message("Raw district name: ", d)
  
  fname <- str_replace_all(d, "[^[:alnum:]_]", "_")
  fp    <- file.path(output_dir, paste0(fname, ".csv"))
  message("Output path: ", fp)
  
  if (file.exists(fp)) {
    message("Skipping (file already exists).")
    next
  }
  
  # build the search term and log it
  search_term <- case_when(
    str_detect(d, regex("district$", ignore_case = TRUE)) ~
      str_replace(d, regex("district$", ignore_case = TRUE), "New Zealand"),
    str_detect(d, regex("city$", ignore_case = TRUE)) ~
      paste(d, "New Zealand"),
    TRUE ~ d
  )
  message(" → search_term = '", search_term, "'")
  
  # wrap get_widget in tryCatch so errors get printed but don't stop the loop
  widget <- tryCatch({
    get_widget(search_term)
  }, error = function(e) {
    message("ERROR in get_widget(): ", e$message)
    return(NULL)
  })
  if (is.null(widget)) next
  
  # same for download_csv
  df <- tryCatch({
    download_csv(widget)
  }, error = function(e) {
    message("ERROR in download_csv(): ", e$message)
    return(NULL)
  })
  if (is.null(df)) next
  
  write_csv(df, fp)
  message("✅ Saved: ", fp)
}