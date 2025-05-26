# Record start time
start_time <- Sys.time()
message("Process started at: ", start_time)

# install / load libraries 
# "furrr" for parallel processing, 
# "future" for future plans,
# "ggplot2" for plotting, 
# "tidyverse" for data manipulation,
# "lubridate" for date handling, 
# "janitor" for data cleaning,
# "recipes" for preprocessing, 
# "warp" for time series analysis,
# "tidymodels" for modeling, 
# "backports" for compatibility,
# "DiceDesign" for design of experiments, 
# "infer" for statistical inference,
# "timetk" for time series feature engineering, 
# "sparsevctrs" for sparse vectors 
# "fs" for file system operations
# "labeling" for labeling axes in plots
#
# On my windows install the dependencies are not being picked up properly
# even with the flag set, so they have been added here
required_pkgs <- c(
  "furrr", "future", "ggplot2", "tidyverse", "lubridate", "janitor", "recipes", 
  "warp", "tidymodels", "backports", "DiceDesign", "infer", "timetk",
  "sparsevctrs", "fs", "labeling"
)
# Check for missing packages and install them if necessary
missing_pkgs <- setdiff(required_pkgs, rownames(installed.packages()))
if (length(missing_pkgs)) 
  install.packages(missing_pkgs, repos = "https://cloud.r-project.org")
# Ensure they load in a reproducible order, hide the output
invisible(lapply(required_pkgs, library, character.only = TRUE))

# Set seed for reproducibility
set.seed(123)

# Set up parallel processing
plan(multisession, workers = parallel::detectCores() - 2)

# Toggle development mode: TRUE for only 10 areas and simplified settings
# Change this to FALSE will result in a longer runtime and full processing
development_mode <- FALSE

# Model & feature settings based on mode 
# lag_spec is a list defining the lags for each feature
# the values are the number of months to lag the feature
if (development_mode) {
  # Development mode settings
  model_params <- list(trees = 100, tree_depth = 4, learn_rate = 0.1, sample_size = 0.5)
  fh_years     <- 1
  group_limit  <- 10
  # Reduce lag for development mode
  lag_spec <- list(
    value = 1:6,
    mort_rate = 0:2,
    temperature_2m_mean = 0:2,
    temperature_2m_max  = 0:2,
    temperature_2m_min  = 0:2,
    sunshine_duration   = 0:2,
    precipitation_sum   = 0:2,
    snowfall_sum        = 0:2
  )
} else {
  # Production mode settings, requires much more processing
  model_params <- list(trees = 500, tree_depth = 6, learn_rate = 0.05, sample_size = 0.8)
  fh_years     <- 1
  group_limit  <- Inf
  # Define lags for all features, up to 6 months for value and 0-12 for mort_rate
  # and 0-6 for the weather features
  lag_spec <- list(
    value = 1:6,
    mort_rate = 0:12,
    temperature_2m_mean = 0:6,
    temperature_2m_max  = 0:6,
    temperature_2m_min  = 0:6,
    sunshine_duration   = 0:6,
    precipitation_sum   = 0:6,
    snowfall_sum        = 0:6
  )
}

########################################
# 1. Load ALL monthly consent sub-series 
########################################
if (!exists("consents_all")) {
  message("Loading consents data...")
  path <- "./datasets/building_consents/Building_consents_by_territorial_authority_(Monthly)_trimmed.csv"
  consents_csv <- read_csv(path,
                           col_types = cols(period = col_character()),
                           show_col_types = FALSE
  ) %>% clean_names()
  consents_csv_2 <- consents_csv %>%
    transmute(
      territorial_authority = series_title_1,
      category              = series_title_2,
      # force exactly two decimals
      period_str = sprintf("%.2f", period),  
      value      = data_value
    ) %>%
    separate(period_str, into = c("year","month"), sep = "\\.", convert = TRUE)
  consents_csv_3 <- consents_csv_2 %>%
    mutate(date = make_date(year, month, 1)) %>%
    select(territorial_authority, category, date, value)
  
  
  # only load if not already in memory
  consents_all <- consents_csv_3
} else {
  message("Consents data already loaded.")
}

######################################
# 2. Load mortgage & weather (monthly)
######################################
# only load if not already in memory
if (!exists("mortgage_monthly")) {
  message("Loading mortgage data...")
  mortgage_monthly <- read_csv(
    "./datasets/mortgage_rates/rbnz_gov_2005-2025_mortgage_rates_hb20.csv",
    skip = 4, show_col_types = FALSE
  ) %>%
    rename(date_label = `Series Id`) %>%
    transmute(
      date      = parse_date_time(date_label, "b Y") %>% floor_date("month"),
      mort_rate = as.numeric(MTGE.MBI.F)
    ) %>%
    drop_na()
} else {
  message("Mortgage data already loaded.")
}

# only load if not already in memory
if (!exists("weather_monthly")) {
  message("Loading weather data...")
  tmp_weather_fun <- function(f) {
    region <- f %>% basename() %>% str_remove_all("(weather-|\\.csv)") %>% str_replace_all("_"," ")
    read_csv(f, show_col_types = FALSE) %>%
      mutate(
        date                  = ymd(time) %>% floor_date("month"),
        territorial_authority = region
      ) %>%
      group_by(territorial_authority, date) %>%
      summarise(
        temperature_2m_mean  = mean(temperature_2m_mean,  na.rm = TRUE),
        temperature_2m_max   = mean(temperature_2m_max,   na.rm = TRUE),
        temperature_2m_min   = mean(temperature_2m_min,   na.rm = TRUE),
        sunshine_duration    = sum(sunshine_duration,     na.rm = TRUE),
        precipitation_sum    = sum(precipitation_sum,     na.rm = TRUE),
        snowfall_sum         = sum(snowfall_sum,          na.rm = TRUE),
        .groups = "drop"
      )
  }
  weather_files   <- list.files("./datasets/weather", pattern = "^weather-.*\\.csv$", full.names = TRUE)
  weather_monthly <- future_map_dfr(
    weather_files,
    tmp_weather_fun,
    .options = furrr_options(seed = TRUE),
    .progress = TRUE
  )
} else {
  message("Weather data already loaded.")
}
#################
# 3. Combine data
#################
# Only do this if not already in memory
if (!exists("data_monthly_all")) {
  message("Combining all data...")
  data_monthly_all <- consents_all %>%
    left_join(mortgage_monthly, by = "date") %>%
    left_join(weather_monthly,   by = c("territorial_authority","date")) %>%
    arrange(territorial_authority, category, date) %>%
    drop_na(mort_rate, temperature_2m_mean)
  # Convert date to Date type
  data_monthly_all <- data_monthly_all %>%
    mutate(date = as.Date(date))
} else {
  message("Data already combined.")
}
########################
# 4. Feature engineering
########################
make_features <- function(df) {
  df %>%
    arrange(date) %>%
    tk_augment_lags(value,               .lags = lag_spec$value) %>%
    tk_augment_lags(mort_rate,           .lags = lag_spec$mort_rate) %>%
    tk_augment_lags(temperature_2m_mean, .lags = lag_spec$temperature_2m_mean) %>%
    tk_augment_lags(temperature_2m_max,  .lags = lag_spec$temperature_2m_max) %>%
    tk_augment_lags(temperature_2m_min,  .lags = lag_spec$temperature_2m_min) %>%
    tk_augment_lags(sunshine_duration,   .lags = lag_spec$sunshine_duration) %>%
    tk_augment_lags(precipitation_sum,   .lags = lag_spec$precipitation_sum) %>%
    tk_augment_lags(snowfall_sum,        .lags = lag_spec$snowfall_sum) %>%
    tk_augment_slidify(value, .period = 3, .f = mean, .names = "roll3") %>%
    mutate(
      year  = year(date),
      month = month(date),
      index = row_number()
    )
}


###############
# 5. Model loops
###############
# Global variables for the model loop
rec <- recipe(value ~ ., data = data_monthly_all) %>%
  update_role(date, territorial_authority, category, new_role = "id") %>%
  step_timeseries_signature(date) %>%
  # Add Fourier series for annual seasonality (12-month cycle)
  step_fourier(date, period = 12, K = 2) %>%
  # Add Fourier series for quarterly seasonality (3-month cycle)
  step_fourier(date, period = 3, K = 1) %>%
  step_rm(territorial_authority, category, date, matches("iso_|\\.stamp")) %>%
  step_dummy(all_nominal_predictors(), one_hot = TRUE) %>%
  step_mutate(across(where(is.logical), as.numeric)) %>%
  step_zv(all_predictors()) %>%
  step_normalize(all_numeric_predictors())
groups <- data_monthly_all %>% 
  distinct(territorial_authority, category) %>% 
  slice_head(n = group_limit)
###################################################
# Sliding window CV for each (TA × category) series
###################################################
all_cv_metrics <- future_pmap_dfr(
  list(groups$territorial_authority, groups$category),
  function(ta, cat) {
    df   <- data_monthly_all %>% 
      filter(territorial_authority == ta, category == cat)
    feat <- make_features(df)
    print(names(feat))
    # 1) build rolling‐window splits
    cv_splits <- time_series_cv(
      data       = feat,
      date_var   = date,
      initial    = "60 months",
      assess     = "12 months",
      skip       = "12 months",
      cumulative = FALSE
    )
    
    # 2) define  model spec
    
    spec <- boost_tree(
      trees       = model_params$trees,
      tree_depth  = model_params$tree_depth,
      learn_rate  = model_params$learn_rate,
      sample_size = model_params$sample_size
    ) %>%
      set_engine("xgboost") %>%
      set_mode("regression")
    
    wf <- workflow() %>%
      add_recipe(rec) %>%
      add_model(spec)
    
    # 3) fit across all rolling windows
    resamples_fitted <- fit_resamples(
      wf,
      resamples = cv_splits,
      metrics   = metric_set(rmse, mae, rsq),
      control   = control_resamples(
        verbose   = TRUE,
        save_pred = TRUE
      )
    )
    
    # 4) collect & average the metrics per series
    cv_metrics <- resamples_fitted %>%
      collect_metrics() %>%                         # yields columns .metric, .estimator, mean, std_err, n
      filter(.metric %in% c("rmse", "mae", "rsq")) %>% 
      select(.metric, mean) %>%                     # keep only the metric name and its aggregated mean
      pivot_wider(
        names_from   = .metric, 
        values_from  = mean
      ) %>%
      rename(
        rmse_mean = rmse,
        mae_mean  = mae,
        rsq_mean  = rsq
      ) %>%
      mutate(
        territorial_authority = ta,
        category              = cat
      )
    
    return(cv_metrics)
  },
  .options  = furrr_options(seed = TRUE),
  .progress  = TRUE
)

print(all_cv_metrics)

write_csv(all_cv_metrics, "./results/cv_metrics_by_series.csv")
##########################################
# Traing Model up to 2023 and predict 2024
##########################################

# For each series, train ≤2023 and predict 2024 → collect preds & actuals
preds_2024_all <- future_pmap_dfr(
  list(groups$territorial_authority, groups$category),
  function(ta, cat) {
    # full series
    df_full <- data_monthly_all %>%
      filter(territorial_authority == ta, category == cat)
    
    # 1) Train on data < 2024-01-01
    train_df  <- df_full %>% filter(date < as.Date("2024-01-01"))
    feat_train <- make_features(train_df)
    wf_hold <- workflow() %>%
      add_recipe(rec) %>%
      add_model(
        boost_tree(
          trees       = model_params$trees,
          tree_depth  = model_params$tree_depth,
          learn_rate  = model_params$learn_rate,
          sample_size = model_params$sample_size
        ) %>%
          set_engine("xgboost") %>%
          set_mode("regression")
      ) %>%
      fit(feat_train)
    
    # 2) Build a 2024 future window (Jan–Dec) using real exogenous data
    future_dates <- seq.Date(as.Date("2024-01-01"), by = "month", length.out = 12)
    future_df <- tibble(
      territorial_authority = ta,
      category              = cat,
      date                  = future_dates,
      value                 = NA_real_
    ) %>%
      # bind actual mortgage & weather for those months:
      left_join(
        mortgage_monthly %>% filter(date %in% future_dates),
        by = "date"
      ) %>%
      left_join(
        weather_monthly %>% filter(territorial_authority == ta, date %in% future_dates),
        by = c("territorial_authority","date")
      )
    
    feat_future <- make_features(future_df)
    preds_2024 <- predict(wf_hold, new_data = feat_future) %>%
      bind_cols(future_df %>% select(date, territorial_authority, category)) %>%
      rename(.pred = .pred)
    
    # 3) Join to true 2024 values
    actual_2024 <- df_full %>%
      filter(between(date, as.Date("2024-01-01"), as.Date("2024-12-01"))) %>%
      select(date, actual = value)
    
    compare <- preds_2024 %>%
      left_join(actual_2024, by = "date")
    
    return(compare)
  },
  .options = furrr_options(seed = TRUE),
  .progress = TRUE
)

# B) Compute pure hold‐out metrics
holdout_metrics <- preds_2024_all %>%
  group_by(territorial_authority, category) %>%
  summarise(
    rmse_2024 = rmse_vec(truth = actual, estimate = .pred),
    mae_2024  = mae_vec(truth = actual, estimate = .pred),
    rsq_2024  = rsq_vec(truth = actual, estimate = .pred),
    .groups   = "drop"
  )

write_csv(holdout_metrics, "./results/holdout_metrics_2024.csv")


# C) Plot & save diagnostics for each series
dir_create("plots/2024_holdout")

preds_2024_all %>%
  group_by(territorial_authority, category) %>%
  group_walk(~{
    df   <- .x
    ta   <- .y$territorial_authority
    catg <- .y$category
    
    # 1) Time‐series: Actual vs Predicted
    p1 <- ggplot(df, aes(x = date)) +
      geom_line(aes(y = actual, color = "Actual"),   linewidth = 1) +
      geom_line(aes(y = .pred,  color = "Predicted"),linewidth = 1) +
      labs(
        title = paste("2024 Actual vs Predicted —", ta, "/", catg),
        x     = "Date", y = "Value", color = NULL
      ) +
      theme_minimal() +
      theme(
        legend.position    = "bottom",
        panel.background   = element_rect(fill = "white", color = NA),
        plot.background    = element_rect(fill = "white", color = NA)
      )
    
    ggsave(
      filename = paste0("plots/2024_holdout/ts_act_vs_pred_",
                        str_replace_all(ta, "\\s+", "_"), "_",
                        str_replace_all(catg, "\\s+", "_"), ".png"),
      plot   = p1,
      width  = 10,
      height = 6,
      bg     = "white"
    )
    
    # 2) Scatter: Predicted vs Actual
    p2 <- ggplot(df, aes(x = actual, y = .pred)) +
      geom_abline(slope = 1, intercept = 0, linetype = "dashed") +
      geom_point(alpha = 0.7) +
      labs(
        title = paste("2024 Predicted vs Actual —", ta, "/", catg),
        x     = "Actual", y = "Predicted"
      ) +
      theme_minimal() +
      theme(
        panel.background   = element_rect(fill = "white", color = NA),
        plot.background    = element_rect(fill = "white", color = NA)
      )
    
    ggsave(
      filename = paste0("plots/2024_holdout/scatter_",
                        str_replace_all(ta, "\\s+", "_"), "_",
                        str_replace_all(catg, "\\s+", "_"), ".png"),
      plot   = p2,
      width  = 6,
      height = 6,
      bg     = "white"
    )
    
    # 3) Histogram: Residuals
    df2 <- df %>% mutate(residual = .pred - actual)
    p3 <- ggplot(df2, aes(x = residual)) +
      geom_histogram(bins = 20, alpha = 0.8) +
      labs(
        title = paste("2024 Residuals —", ta, "/", catg),
        x     = "Prediction − Actual", y = "Count"
      ) +
      theme_minimal() +
      theme(
        panel.background   = element_rect(fill = "white", color = NA),
        plot.background    = element_rect(fill = "white", color = NA)
      )
    
    ggsave(
      filename = paste0("plots/2024_holdout/hist_resid_",
                        str_replace_all(ta, "\\s+", "_"), "_",
                        str_replace_all(catg, "\\s+", "_"), ".png"),
      plot   = p3,
      width  = 8,
      height = 6,
      bg     = "white"
    )
  })

# Record end time and duration
end_time <- Sys.time()
message("Process completed at: ", end_time)
message("Total duration: ", difftime(end_time, start_time, units = "mins"), " minutes")