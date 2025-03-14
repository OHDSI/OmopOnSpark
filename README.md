
<!-- README.md is generated from README.Rmd. Please edit that file -->

# OmopSparkConnector

<!-- badges: start -->

[![R-CMD-check](https://github.com/oxford-pharmacoepi/OmopSparkConnector/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/oxford-pharmacoepi/OmopSparkConnector/actions/workflows/R-CMD-check.yaml)
<!-- badges: end -->

OmopSparkConnector provides a Spark specific implementation of an OMOP
CDM reference as defined by the omopgenerics R package.

## Installation

You can install the development version of OmopSparkConnector from
[GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
devtools::install_github("oxford-pharmacoepi/OmopSparkConnector")
```

## Creating a cdm reference using Sparklyr

Let’s first load the R libraries.

``` r
library(dplyr)
#> 
#> Attaching package: 'dplyr'
#> The following objects are masked from 'package:stats':
#> 
#>     filter, lag
#> The following objects are masked from 'package:base':
#> 
#>     intersect, setdiff, setequal, union
library(sparklyr)
#> Warning: package 'sparklyr' was built under R version 4.4.3
#> 
#> Attaching package: 'sparklyr'
#> The following object is masked from 'package:stats':
#> 
#>     filter
library(OmopSparkConnector)
```

To work with OmopSparkConnector, we will first need to create a
connection to our data using the sparklyr. In the example below, we have
a schema called “omop” that contains all the OMOP CDM tables and then we
have another schema where we can write results during the course of a
study. We also set a write prefix so that all the tables we write start
with this (which makes it easy to clean up afterwards and avoid any name
conflicts with other users).

``` r
con <- sparklyr::spark_connect(.....)
cdm <- cdmFromSpark(con, 
             cdmSchema = "omop", 
             writeSchema = "results", 
             writePrefix = "study_1_")
```

For this introduction we’ll use a mock cdm where we have a small
synthetic dataset in a local spark database.

``` r
cdm <- mockSparkCdm(path = file.path(tempdir(), "temp_spark"))
#> ! Validation has been turned off, this is not recommended as analytical
#>   packages assumed the cdm_reference object fulfills the cdm validation
#>   criteria.
#> ! Validation has been turned off, this is not recommended as analytical
#>   packages assumed the cdm_reference object fulfills the cdm validation
#>   criteria.
cdm
#> 
#> ── # OMOP CDM reference (sparklyr) of mock local spark ─────────────────────────
#> • omop tables: cdm_source, concept, concept_ancestor, concept_relationship,
#> concept_synonym, condition_occurrence, drug_strength, observation_period,
#> person, vocabulary
#> • cohort tables: -
#> • achilles tables: -
#> • other tables: -
```

## Cross platform support

With our cdm reference created, we now a single object in R that
represents our OMOP CDM data.

``` r
cdm$person |> 
  dplyr::glimpse()
#> Rows: ??
#> Columns: 18
#> Database: spark_connection
#> $ person_id                   <int> 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
#> $ gender_concept_id           <int> 8507, 8532, 8532, 8532, 8507, 8507, 8532, …
#> $ year_of_birth               <int> 1974, 1963, 1965, 1987, 1975, 1956, 1980, …
#> $ month_of_birth              <int> 11, 8, 1, 3, 8, 8, 12, 12, 7, 1
#> $ day_of_birth                <int> 28, 16, 30, 26, 29, 19, 27, 14, 30, 19
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

cdm$observation_period |> 
  dplyr::glimpse()
#> Rows: ??
#> Columns: 5
#> Database: spark_connection
#> $ observation_period_id         <int> 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
#> $ person_id                     <int> 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
#> $ observation_period_start_date <date> 1994-06-09, 1992-11-29, 2004-07-05, 2009…
#> $ observation_period_end_date   <date> 1996-07-31, 2009-06-25, 2008-01-05, 2011…
#> $ period_type_concept_id        <int> NA, NA, NA, NA, NA, NA, NA, NA, NA, NA
```

We can also make use of various existing packages that work with a cdm
reference. For example, we can extract a summary of our database using
the OmopSketch package.

``` r
# library(OmopSketch)
# library(flextable)
# snapshot <- summariseObservationPeriod(cdm$observation_period)
# tableObservationPeriod(snapshot, type = "flextable")
```

Or we can make a cohort of individuals aged between 18 and 65 using the
CohortConstructor package.

``` r
# library(CohortConstructor)
# cdm$working_age <- demographicsCohort(cdm,
#                                       name = "working_age",
#                                       ageRange = c(18, 65))
# cdm$working_age
```

## Native spark support

We can also make use of native spark queries. For example we can compute
summary statistics on one of our cdm tables using spark functions.

``` r
cdm$person |> 
  sdf_describe(cols = c("gender_concept_id",
                        "year_of_birth",
                        "month_of_birth",
                        "day_of_birth"))
#> # Source:   table<`sparklyr_tmp_cb2e998f_e7f3_48d2_8f79_1b925d79a939`> [?? x 5]
#> # Database: spark_connection
#>   summary gender_concept_id  year_of_birth     month_of_birth    day_of_birth   
#>   <chr>   <chr>              <chr>             <chr>             <chr>          
#> 1 count   10                 10                10                10             
#> 2 mean    8522.0             1971.5            7.1               23.8           
#> 3 stddev  12.909944487358048 12.64252084567525 4.175324338699131 6.142746399887…
#> 4 min     8507               1956              1                 14             
#> 5 max     8532               1994              12                30
```

With this we are hopefully achieving the best of both worlds. On the one
hand we can participate in network studies where code has been written
in such a way to work across database platforms. And then on the other
we are able to go beyond this approach, writing bespoke code that makes
use of Spark-specific functionality.

# Disconnecting from your spark connection

We can disconnect from our spark connection like so

``` r
cdmDisconnect(cdm)
```
