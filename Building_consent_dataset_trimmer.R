# Building Consent Dataset Trimmer
# This script reads a CSV file containing building consent data, filters out
# New Zealand data and any data before 2005, and writes the filtered data to a new CSV file.
# Original file is 700+ megabytes

# 0.  Install / load libraries
required_pkgs <- c(
  "data.table"
)
missing_pkgs  <- required_pkgs[!(required_pkgs %in% rownames(installed.packages()))]
if (length(missing_pkgs)) {
  message("Installing missing packages: ", paste(missing_pkgs, collapse = ", "))
  install.packages(missing_pkgs,
                   repos       = "https://cloud.r-project.org",
                   dependencies = TRUE)
}
# Ensure they load in a reproducible order 
invisible(lapply(required_pkgs, library, character.only = TRUE))

# 1. Define file paths
infile <- "./datasets/building_consents/Building_consents_by_territorial_authority_(Monthly).csv"
outfile <- "./datasets/building_consents/Building_consents_by_territorial_authority_(Monthly)_trimmed.csv"

# 2. Read the entire CSV
dt <- fread(infile, stringsAsFactors = FALSE)

# 3. Filter to exclude New Zealand and years before 2005 and only keep "Number" units
dt_filtered <- dt[
  Period > "2004.12" & 
    Series_title_1 != "New Zealand" &
    Units == "Number" &
    Series_title_3 == "New"
]


# 4. Write out the result
fwrite(dt_filtered, outfile)