
<!-- README.md is generated from README.Rmd. Please edit that file -->

# OmopSparkConnector

<!-- badges: start -->

[![R-CMD-check](https://github.com/oxford-pharmacoepi/OmopSparkConnector/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/oxford-pharmacoepi/OmopSparkConnector/actions/workflows/R-CMD-check.yaml)
<!-- badges: end -->

The goal of OmopSparkConnector is to …

## Installation

You can install the development version of OmopSparkConnector from
[GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
devtools::install_github("oxford-pharmacoepi/OmopSparkConnector")
```

## Example

``` r
library(OmopSparkConnector)
cdm <- mockSparkCdm(path = file.path(tempdir(), "temp_spark"))
#> Re-using existing Spark connection to local
#> ! Validation has been turned off, this is not recommended as analytical
#>   packages assumed the cdm_reference object fulfills the cdm validation
#>   criteria.
#> ! As no names where provided, it was assumed `cdmSchema = c(schema = 'omop')`
#> ! As no names where provided, it was assumed `writeSchema = c(schema = 'omop')`
#> ! Validation has been turned off, this is not recommended as analytical
#>   packages assumed the cdm_reference object fulfills the cdm validation
#>   criteria.
cdm$person |> 
  dplyr::glimpse()
#> Rows: ??
#> Columns: 18
#> Database: spark_connection
#> $ person_id                   <int> 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
#> $ gender_concept_id           <int> 8507, 8532, 8532, 8532, 8507, 8507, 8507, …
#> $ year_of_birth               <int> 1981, 1953, 1961, 1959, 1971, 1992, 1968, …
#> $ month_of_birth              <int> 5, 12, 8, 7, 11, 4, 6, 10, 12, 6
#> $ day_of_birth                <int> 29, 3, 27, 21, 15, 28, 29, 10, 30, 19
#> $ race_concept_id             <int> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA
#> $ ethnicity_concept_id        <int> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA
#> $ birth_datetime              <dttm> 1970-01-01 01:00:00, 1970-01-01 01:00:00, …
#> $ location_id                 <int> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA
#> $ provider_id                 <int> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA
#> $ care_site_id                <int> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA
#> $ person_source_value         <chr> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA
#> $ gender_source_value         <chr> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA
#> $ gender_source_concept_id    <int> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA
#> $ race_source_value           <chr> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA
#> $ race_source_concept_id      <int> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA
#> $ ethnicity_source_value      <chr> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA
#> $ ethnicity_source_concept_id <int> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA
```

``` r
cdmDisconnect(cdm)
```
