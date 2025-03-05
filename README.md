
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
cdm_local <- omock::mockCdmReference() |>
  omock::mockPerson(nPerson = 10) |>
  omock::mockObservationPeriod() |>
  omock::mockConditionOccurrence()

folder <- file.path(tempdir(), "temp_spark")
working_config <- sparklyr::spark_config()
working_config$spark.sql.warehouse.dir <- folder
con <- sparklyr::spark_connect(master = "local", config = working_config)
createSchema(con = con, schema = list(schema = "omop"))
src <- sparkSource(con = con, 
                   writeSchema = list(schema = "omop"))
cdm <- insertCdmTo(cdm_local, src)
#> ! Validation has been turned off, this is not recommended as analytical
#>   packages assumed the cdm_reference object fulfills the cdm validation
#>   criteria.
cdm
#> 
#> ── # OMOP CDM reference (sparklyr) of mock database ────────────────────────────
#> • omop tables: cdm_source, concept, concept_ancestor, concept_relationship,
#> concept_synonym, condition_occurrence, drug_strength, observation_period,
#> person, vocabulary
#> • cohort tables: -
#> • achilles tables: -
#> • other tables: -
cdm$person |> 
  dplyr::glimpse()
#> Rows: ??
#> Columns: 18
#> Database: spark_connection
#> $ person_id                   <int> 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
#> $ gender_concept_id           <int> 8532, 8507, 8532, 8532, 8532, 8507, 8507, …
#> $ year_of_birth               <int> 1973, 1955, 1965, 1968, 1978, 1998, 1979, …
#> $ month_of_birth              <int> 10, 12, 4, 11, 12, 8, 2, 1, 9, 8
#> $ day_of_birth                <int> 22, 9, 5, 29, 18, 15, 23, 2, 17, 25
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
