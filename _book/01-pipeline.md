# Pipeline Explanation

This page provides comprehensive explanation for the GDP data processing R script.


---

## Data Processing ETL Pipeline


``` r
  # Extract 
get_unsd_gdp_data() %>%
  get_taiwan_gdp_data() %>%
  
  # Transform
  compute_missing_values() %>%
  delete_data_out_of_valid_range() %>%
  add_economy_labels() %>%
  compute_aggregate_values() %>%
  add_notes() %>%
  
  # Load
  export_to_general_csv("gdp_update.csv")
  export_to_usis_csv("gdp_update_usis.csv")
```

---

## Functions 

### `get_unsd_gdp_data()` {-}
- Downloads GDP data using UNSD API (both constant and current prices).
- Merges, reshapes, and formats the dataset.

### `get_taiwan_gdp_data(df)` {-}
- Downloads GDP data from Taiwan NSO (both constant and current prices).
- Rebases the GDP at constant prices from the year 2021 to 2015.
- Calculates TWD to USD exchange rates.
    + GDP data at current prices in USD are converted from TWD using annual period-average exchange rates.
    + GDP data in constant prices in USD are converted from TWD using the annual period-average exchange rate of the base year (2015) for all years.

### `compute_missing_values(df)` {-}
- Handles historical and geopolitical inconsistencies by merging country records (e.g., Yugoslavia, USSR).
- Aggregates GDP for dissolved economies.

### `delete_data_out_of_valid_range(df)` {-}
- Filters out data points falling outside the valid year range for each country.

### `compute_aggregate_values(df)` {-}
- Computes GDP aggregates for groups using hierarchical mappings.

### `add_economy_labels(df)` {-}
- Joins human-readable economy labels using economy codes.

### `add_comments(df)` {-}
- Adds "CommentEN" and "CommentFR" columns remarking on values which were calculated in `compute_missing_values(df)`.

### `export_to_generic_csv(df, filename)` {-}
- Saves the dataset to a generic CSV file.

### `export_to_usis_csv(df, filename)` {-}
- Saves the dataset to a CSV file used by USIS for upload to UNCTADstat.

---
