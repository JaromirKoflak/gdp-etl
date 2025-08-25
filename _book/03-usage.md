# Usage

## ETL Pipeline

The ETL script can be found in `gdp_etl_pipelive.R`. The file contains one function which runs the entire ETL pipeline.

## Settings {-}

### Working Directory {-}
The script automatically sets the working directory to the location of the current R script file.

### Directories {-}
Defines paths for:

- `datadir`: Data input
- `outputdir`: Output files

### Country Metadata {-}
- `Dim_countries.csv`: Contains economy codes and valid year ranges.
- `Dim_Countries_Hierarchy_All.csv`: Contains hierarchical grouping for aggregations.
- `lab_all.csv`: Contains economy codes and labels 

## Comparison Plots

Script for generating comparison plots can be found in 
