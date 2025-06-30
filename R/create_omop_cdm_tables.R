#' Create OMOP CDM tables
#'
#' @param con Connection. Must be via odbc (sparklyr connection not supported).
#' @param schema_name Schema in which to create tables
#' @param cdm_version Which version of the OMOP CDM to create. Can be "5.3" or
#' "5.4"
#' @param overwrite Whether to overwrite existing tables
#' @param id_bigint Whether to use big integers
#'
#' @return OMOP CDM tables created in database
#' @export
#'
create_omop_cdm_tables <- function(con,
                                   schema_name,
                                   cdm_version = "5.4",
                                   overwrite = FALSE,
                                   id_bigint = FALSE) {
  if (con_type(con) != "odbc") {
    cli::cli_abort("Currently, only ODBC connections are supported")
  }
  omopgenerics::assertChoice(cdm_version, choices = c("5.3", "5.4"), length = 1)


  create_person(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_observation_period(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_visit_occurrence(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_visit_detail(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_condition_occurrence(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_drug_exposure(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_procedure_occurrence(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_device_exposure(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_measurement(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_observation(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_death(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_note(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_note_nlp(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_specimen(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_fact_relationship(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_location(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_care_site(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_provider(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_payer_plan_period(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_cost(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_drug_era(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_dose_era(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_condition_era(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_episode(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_episode_event(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_metadata(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_cdm_source(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_concept(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_vocabulary(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_domain(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_concept_class(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_concept_relationship(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_relationship(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_concept_synonym(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_concept_ancestor(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_source_to_concept_map(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_drug_strength(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_cohort(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
  create_cohort_definition(con = con, schema_name = schema_name, cdm_version = cdm_version, overwrite = overwrite, id_bigint = id_bigint)
}

# helpers
create_sql <- function(overwrite) {
  if (overwrite) {
    "CREATE OR REPLACE TABLE"
  } else {
    "CREATE TABLE"
  }
}
id_int_type <- function(id_bigint) {
  if (id_bigint) "BIGINT" else "INTEGER"
}
schema_prefix <- function(schema) {
  if (is.null(schema)) {
    ""
  } else {
    paste0(schema, ".")
  }
}

# sql to create each table
create_person <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  DBI::dbExecute(con, glue::glue("--HINT DISTRIBUTE ON KEY (person_id)
{create_sql(overwrite)} {schema_prefix(schema_name)}person
USING DELTA
AS
SELECT
CAST(NULL AS {id_int_type(id_bigint)}) AS person_id,
CAST(NULL AS integer) AS gender_concept_id,
CAST(NULL AS integer) AS year_of_birth,
CAST(NULL AS integer) AS month_of_birth,
CAST(NULL AS integer) AS day_of_birth,
CAST(NULL AS TIMESTAMP) AS birth_datetime,
CAST(NULL AS integer) AS race_concept_id,
CAST(NULL AS integer) AS ethnicity_concept_id,
CAST(NULL AS integer) AS location_id,
CAST(NULL AS integer) AS provider_id,
CAST(NULL AS integer) AS care_site_id,
CAST(NULL AS STRING) AS person_source_value,
CAST(NULL AS STRING) AS gender_source_value,
CAST(NULL AS integer) AS gender_source_concept_id,
CAST(NULL AS STRING) AS race_source_value,
CAST(NULL AS integer) AS race_source_concept_id,
CAST(NULL AS STRING) AS ethnicity_source_value,
CAST(NULL AS integer) AS ethnicity_source_concept_id  WHERE 1 = 0;"))

  cli::cli_alert_success("Created person table")
}
create_observation_period <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  DBI::dbExecute(
    con,
    glue::glue(
      "--HINT DISTRIBUTE ON KEY (person_id)
{create_sql(overwrite)} {schema_prefix(schema_name)}observation_period
  USING DELTA
  AS
  SELECT
  CAST(NULL AS integer) AS observation_period_id,
  CAST(NULL AS {id_int_type(id_bigint)}) AS person_id,
  CAST(NULL AS date) AS observation_period_start_date,
  CAST(NULL AS date) AS observation_period_end_date,
  CAST(NULL AS integer) AS period_type_concept_id  WHERE 1 = 0;"
    )
  )

  cli::cli_alert_success("Created observation period table")
}
create_visit_occurrence <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  if (cdm_version == "5.3") {
    DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON KEY (person_id)
{create_sql(overwrite)} {schema_prefix(schema_name)}visit_occurrence
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS visit_occurrence_id,
CAST(NULL AS integer) AS person_id,
CAST(NULL AS integer) AS visit_concept_id,
CAST(NULL AS date) AS visit_start_date,
CAST(NULL AS TIMESTAMP) AS visit_start_datetime,
CAST(NULL AS date) AS visit_end_date,
CAST(NULL AS TIMESTAMP) AS visit_end_datetime,
CAST(NULL AS integer) AS visit_type_concept_id,
CAST(NULL AS integer) AS provider_id,
CAST(NULL AS integer) AS care_site_id,
CAST(NULL AS STRING) AS visit_source_value,
CAST(NULL AS integer) AS visit_source_concept_id,
CAST(NULL AS integer) AS admitting_source_concept_id,
CAST(NULL AS STRING) AS admitting_source_value,
CAST(NULL AS integer) AS discharge_to_concept_id,
CAST(NULL AS STRING) AS discharge_to_source_value,
CAST(NULL AS integer) AS preceding_visit_occurrence_id  WHERE 1 = 0;"))
  } else {
    DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON KEY (person_id)
{create_sql(overwrite)} {schema_prefix(schema_name)}visit_occurrence
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS visit_occurrence_id,
CAST(NULL AS {id_int_type(id_bigint)}) AS person_id,
CAST(NULL AS integer) AS visit_concept_id,
CAST(NULL AS date) AS visit_start_date,
CAST(NULL AS TIMESTAMP) AS visit_start_datetime,
CAST(NULL AS date) AS visit_end_date,
CAST(NULL AS TIMESTAMP) AS visit_end_datetime,
CAST(NULL AS integer) AS visit_type_concept_id,
CAST(NULL AS integer) AS provider_id,
CAST(NULL AS integer) AS care_site_id,
CAST(NULL AS STRING) AS visit_source_value,
CAST(NULL AS integer) AS visit_source_concept_id,
CAST(NULL AS integer) AS admitted_from_concept_id,
CAST(NULL AS STRING) AS admitted_from_source_value,
CAST(NULL AS integer) AS discharged_to_concept_id,
CAST(NULL AS STRING) AS discharged_to_source_value,
CAST(NULL AS integer) AS preceding_visit_occurrence_id  WHERE 1 = 0;"))
  }



  cli::cli_alert_success("Created visit occurrence table")
}
create_visit_detail <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  if (cdm_version == "5.3") {
    DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON KEY (person_id)
{create_sql(overwrite)} {schema_prefix(schema_name)}visit_detail
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS visit_detail_id,
CAST(NULL AS integer) AS person_id,
CAST(NULL AS integer) AS visit_detail_concept_id,
CAST(NULL AS date) AS visit_detail_start_date,
CAST(NULL AS TIMESTAMP) AS visit_detail_start_datetime,
CAST(NULL AS date) AS visit_detail_end_date,
CAST(NULL AS TIMESTAMP) AS visit_detail_end_datetime,
CAST(NULL AS integer) AS visit_detail_type_concept_id,
CAST(NULL AS integer) AS provider_id,
CAST(NULL AS integer) AS care_site_id,
CAST(NULL AS STRING) AS visit_detail_source_value,
CAST(NULL AS integer) AS visit_detail_source_concept_id,
CAST(NULL AS STRING) AS admitting_source_value,
CAST(NULL AS integer) AS admitting_source_concept_id,
CAST(NULL AS STRING) AS discharge_to_source_value,
CAST(NULL AS integer) AS discharge_to_concept_id,
CAST(NULL AS integer) AS preceding_visit_detail_id,
CAST(NULL AS integer) AS visit_detail_parent_id,
CAST(NULL AS integer) AS visit_occurrence_id  WHERE 1 = 0;"))
  } else {
    DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON KEY (person_id)
{create_sql(overwrite)} {schema_prefix(schema_name)}visit_detail
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS visit_detail_id,
CAST(NULL AS {id_int_type(id_bigint)}) AS person_id,
CAST(NULL AS integer) AS visit_detail_concept_id,
CAST(NULL AS date) AS visit_detail_start_date,
CAST(NULL AS TIMESTAMP) AS visit_detail_start_datetime,
CAST(NULL AS date) AS visit_detail_end_date,
CAST(NULL AS TIMESTAMP) AS visit_detail_end_datetime,
CAST(NULL AS integer) AS visit_detail_type_concept_id,
CAST(NULL AS integer) AS provider_id,
CAST(NULL AS integer) AS care_site_id,
CAST(NULL AS STRING) AS visit_detail_source_value,
CAST(NULL AS integer) AS visit_detail_source_concept_id,
CAST(NULL AS integer) AS admitted_from_concept_id,
CAST(NULL AS STRING) AS admitted_from_source_value,
CAST(NULL AS STRING) AS discharged_to_source_value,
CAST(NULL AS integer) AS discharged_to_concept_id,
CAST(NULL AS integer) AS preceding_visit_detail_id,
CAST(NULL AS integer) AS parent_visit_detail_id,
CAST(NULL AS integer) AS visit_occurrence_id  WHERE 1 = 0;"))
  }


  cli::cli_alert_success("Created visit detail table")
}
create_condition_occurrence <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON KEY (person_id)
{create_sql(overwrite)} {schema_prefix(schema_name)}condition_occurrence
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS condition_occurrence_id,
CAST(NULL AS {id_int_type(id_bigint)}) AS person_id,
CAST(NULL AS integer) AS condition_concept_id,
CAST(NULL AS date) AS condition_start_date,
CAST(NULL AS TIMESTAMP) AS condition_start_datetime,
CAST(NULL AS date) AS condition_end_date,
CAST(NULL AS TIMESTAMP) AS condition_end_datetime,
CAST(NULL AS integer) AS condition_type_concept_id,
CAST(NULL AS integer) AS condition_status_concept_id,
CAST(NULL AS STRING) AS stop_reason,
CAST(NULL AS integer) AS provider_id,
CAST(NULL AS integer) AS visit_occurrence_id,
CAST(NULL AS integer) AS visit_detail_id,
CAST(NULL AS STRING) AS condition_source_value,
CAST(NULL AS integer) AS condition_source_concept_id,
CAST(NULL AS STRING) AS condition_status_source_value  WHERE 1 = 0;"))

  cli::cli_alert_success("Created condition occurrence table")
}
create_drug_exposure <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON KEY (person_id)
{create_sql(overwrite)} {schema_prefix(schema_name)}drug_exposure
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS drug_exposure_id,
CAST(NULL AS {id_int_type(id_bigint)}) AS person_id,
CAST(NULL AS integer) AS drug_concept_id,
CAST(NULL AS date) AS drug_exposure_start_date,
CAST(NULL AS TIMESTAMP) AS drug_exposure_start_datetime,
CAST(NULL AS date) AS drug_exposure_end_date,
CAST(NULL AS TIMESTAMP) AS drug_exposure_end_datetime,
CAST(NULL AS date) AS verbatim_end_date,
CAST(NULL AS integer) AS drug_type_concept_id,
CAST(NULL AS STRING) AS stop_reason,
CAST(NULL AS integer) AS refills,
CAST(NULL AS float) AS quantity,
CAST(NULL AS integer) AS days_supply,
CAST(NULL AS STRING) AS sig,
CAST(NULL AS integer) AS route_concept_id,
CAST(NULL AS STRING) AS lot_number,
CAST(NULL AS integer) AS provider_id,
CAST(NULL AS integer) AS visit_occurrence_id,
CAST(NULL AS integer) AS visit_detail_id,
CAST(NULL AS STRING) AS drug_source_value,
CAST(NULL AS integer) AS drug_source_concept_id,
CAST(NULL AS STRING) AS route_source_value,
CAST(NULL AS STRING) AS dose_unit_source_value  WHERE 1 = 0;"))

  cli::cli_alert_success("Created drug exposure table")
}
create_procedure_occurrence <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  if (cdm_version == "5.3") {
    DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON KEY (person_id)
{create_sql(overwrite)} {schema_prefix(schema_name)}procedure_occurrence
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS procedure_occurrence_id,
CAST(NULL AS integer) AS person_id,
CAST(NULL AS integer) AS procedure_concept_id,
CAST(NULL AS date) AS procedure_date,
CAST(NULL AS TIMESTAMP) AS procedure_datetime,
CAST(NULL AS integer) AS procedure_type_concept_id,
CAST(NULL AS integer) AS modifier_concept_id,
CAST(NULL AS integer) AS quantity,
CAST(NULL AS integer) AS provider_id,
CAST(NULL AS integer) AS visit_occurrence_id,
CAST(NULL AS integer) AS visit_detail_id,
CAST(NULL AS STRING) AS procedure_source_value,
CAST(NULL AS integer) AS procedure_source_concept_id,
CAST(NULL AS STRING) AS modifier_source_value  WHERE 1 = 0;"))
  } else {
    DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON KEY (person_id)
{create_sql(overwrite)} {schema_prefix(schema_name)}procedure_occurrence
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS procedure_occurrence_id,
CAST(NULL AS {id_int_type(id_bigint)}) AS person_id,
CAST(NULL AS integer) AS procedure_concept_id,
CAST(NULL AS date) AS procedure_date,
CAST(NULL AS TIMESTAMP) AS procedure_datetime,
CAST(NULL AS date) AS procedure_end_date,
CAST(NULL AS TIMESTAMP) AS procedure_end_datetime,
CAST(NULL AS integer) AS procedure_type_concept_id,
CAST(NULL AS integer) AS modifier_concept_id,
CAST(NULL AS integer) AS quantity,
CAST(NULL AS integer) AS provider_id,
CAST(NULL AS integer) AS visit_occurrence_id,
CAST(NULL AS integer) AS visit_detail_id,
CAST(NULL AS STRING) AS procedure_source_value,
CAST(NULL AS integer) AS procedure_source_concept_id,
CAST(NULL AS STRING) AS modifier_source_value  WHERE 1 = 0;"))
  }


  cli::cli_alert_success("Created procedure occurrence table")
}
create_device_exposure <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  if (cdm_version == "5.3") {
    DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON KEY (person_id)
{create_sql(overwrite)} {schema_prefix(schema_name)}device_exposure
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS device_exposure_id,
CAST(NULL AS integer) AS person_id,
CAST(NULL AS integer) AS device_concept_id,
CAST(NULL AS date) AS device_exposure_start_date,
CAST(NULL AS TIMESTAMP) AS device_exposure_start_datetime,
CAST(NULL AS date) AS device_exposure_end_date,
CAST(NULL AS TIMESTAMP) AS device_exposure_end_datetime,
CAST(NULL AS integer) AS device_type_concept_id,
CAST(NULL AS STRING) AS unique_device_id,
CAST(NULL AS integer) AS quantity,
CAST(NULL AS integer) AS provider_id,
CAST(NULL AS integer) AS visit_occurrence_id,
CAST(NULL AS integer) AS visit_detail_id,
CAST(NULL AS STRING) AS device_source_value,
CAST(NULL AS integer) AS device_source_concept_id  WHERE 1 = 0"))
  } else {
    DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON KEY (person_id)
{create_sql(overwrite)} {schema_prefix(schema_name)}device_exposure
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS device_exposure_id,
CAST(NULL AS {id_int_type(id_bigint)}) AS person_id,
CAST(NULL AS integer) AS device_concept_id,
CAST(NULL AS date) AS device_exposure_start_date,
CAST(NULL AS TIMESTAMP) AS device_exposure_start_datetime,
CAST(NULL AS date) AS device_exposure_end_date,
CAST(NULL AS TIMESTAMP) AS device_exposure_end_datetime,
CAST(NULL AS integer) AS device_type_concept_id,
CAST(NULL AS STRING) AS unique_device_id,
CAST(NULL AS STRING) AS production_id,
CAST(NULL AS integer) AS quantity,
CAST(NULL AS integer) AS provider_id,
CAST(NULL AS integer) AS visit_occurrence_id,
CAST(NULL AS integer) AS visit_detail_id,
CAST(NULL AS STRING) AS device_source_value,
CAST(NULL AS integer) AS device_source_concept_id,
CAST(NULL AS integer) AS unit_concept_id,
CAST(NULL AS STRING) AS unit_source_value,
CAST(NULL AS integer) AS unit_source_concept_id  WHERE 1 = 0;"))
  }


  cli::cli_alert_success("Created device exposure table")
}
create_measurement <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  if (cdm_version == "5.3") {
    DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON KEY (person_id)
{create_sql(overwrite)} {schema_prefix(schema_name)}measurement
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS measurement_id,
CAST(NULL AS integer) AS person_id,
CAST(NULL AS integer) AS measurement_concept_id,
CAST(NULL AS date) AS measurement_date,
CAST(NULL AS TIMESTAMP) AS measurement_datetime,
CAST(NULL AS STRING) AS measurement_time,
CAST(NULL AS integer) AS measurement_type_concept_id,
CAST(NULL AS integer) AS operator_concept_id,
CAST(NULL AS float) AS value_as_number,
CAST(NULL AS integer) AS value_as_concept_id,
CAST(NULL AS integer) AS unit_concept_id,
CAST(NULL AS float) AS range_low,
CAST(NULL AS float) AS range_high,
CAST(NULL AS integer) AS provider_id,
CAST(NULL AS integer) AS visit_occurrence_id,
CAST(NULL AS integer) AS visit_detail_id,
CAST(NULL AS STRING) AS measurement_source_value,
CAST(NULL AS integer) AS measurement_source_concept_id,
CAST(NULL AS STRING) AS unit_source_value,
CAST(NULL AS STRING) AS value_source_value  WHERE 1 = 0;"))
  } else {
    DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON KEY (person_id)
{create_sql(overwrite)} {schema_prefix(schema_name)}measurement
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS measurement_id,
CAST(NULL AS {id_int_type(id_bigint)}) AS person_id,
CAST(NULL AS integer) AS measurement_concept_id,
CAST(NULL AS date) AS measurement_date,
CAST(NULL AS TIMESTAMP) AS measurement_datetime,
CAST(NULL AS STRING) AS measurement_time,
CAST(NULL AS integer) AS measurement_type_concept_id,
CAST(NULL AS integer) AS operator_concept_id,
CAST(NULL AS float) AS value_as_number,
CAST(NULL AS integer) AS value_as_concept_id,
CAST(NULL AS integer) AS unit_concept_id,
CAST(NULL AS float) AS range_low,
CAST(NULL AS float) AS range_high,
CAST(NULL AS integer) AS provider_id,
CAST(NULL AS integer) AS visit_occurrence_id,
CAST(NULL AS integer) AS visit_detail_id,
CAST(NULL AS STRING) AS measurement_source_value,
CAST(NULL AS integer) AS measurement_source_concept_id,
CAST(NULL AS STRING) AS unit_source_value,
CAST(NULL AS integer) AS unit_source_concept_id,
CAST(NULL AS STRING) AS value_source_value,
CAST(NULL AS integer) AS measurement_event_id,
CAST(NULL AS integer) AS meas_event_field_concept_id  WHERE 1 = 0;"))
  }


  cli::cli_alert_success("Created measurement table")
}
create_observation <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  if (cdm_version == "5.3") {
    DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON KEY (person_id)
{create_sql(overwrite)} {schema_prefix(schema_name)}observation
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS observation_id,
CAST(NULL AS integer) AS person_id,
CAST(NULL AS integer) AS observation_concept_id,
CAST(NULL AS date) AS observation_date,
CAST(NULL AS TIMESTAMP) AS observation_datetime,
CAST(NULL AS integer) AS observation_type_concept_id,
CAST(NULL AS float) AS value_as_number,
CAST(NULL AS STRING) AS value_as_string,
CAST(NULL AS integer) AS value_as_concept_id,
CAST(NULL AS integer) AS qualifier_concept_id,
CAST(NULL AS integer) AS unit_concept_id,
CAST(NULL AS integer) AS provider_id,
CAST(NULL AS integer) AS visit_occurrence_id,
CAST(NULL AS integer) AS visit_detail_id,
CAST(NULL AS STRING) AS observation_source_value,
CAST(NULL AS integer) AS observation_source_concept_id,
CAST(NULL AS STRING) AS unit_source_value,
CAST(NULL AS STRING) AS qualifier_source_value  WHERE 1 = 0;"))
  } else {
    DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON KEY (person_id)
{create_sql(overwrite)} {schema_prefix(schema_name)}observation
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS observation_id,
CAST(NULL AS {id_int_type(id_bigint)}) AS person_id,
CAST(NULL AS integer) AS observation_concept_id,
CAST(NULL AS date) AS observation_date,
CAST(NULL AS TIMESTAMP) AS observation_datetime,
CAST(NULL AS integer) AS observation_type_concept_id,
CAST(NULL AS float) AS value_as_number,
CAST(NULL AS STRING) AS value_as_string,
CAST(NULL AS integer) AS value_as_concept_id,
CAST(NULL AS integer) AS qualifier_concept_id,
CAST(NULL AS integer) AS unit_concept_id,
CAST(NULL AS integer) AS provider_id,
CAST(NULL AS integer) AS visit_occurrence_id,
CAST(NULL AS integer) AS visit_detail_id,
CAST(NULL AS STRING) AS observation_source_value,
CAST(NULL AS integer) AS observation_source_concept_id,
CAST(NULL AS STRING) AS unit_source_value,
CAST(NULL AS STRING) AS qualifier_source_value,
CAST(NULL AS STRING) AS value_source_value,
CAST(NULL AS integer) AS observation_event_id,
CAST(NULL AS integer) AS obs_event_field_concept_id  WHERE 1 = 0;"))
  }


  cli::cli_alert_success("Created observation table")
}
create_death <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON KEY (person_id)
{create_sql(overwrite)} {schema_prefix(schema_name)}death
USING DELTA
AS
SELECT
CAST(NULL AS {id_int_type(id_bigint)}) AS person_id,
CAST(NULL AS date) AS death_date,
CAST(NULL AS TIMESTAMP) AS death_datetime,
CAST(NULL AS integer) AS death_type_concept_id,
CAST(NULL AS integer) AS cause_concept_id,
CAST(NULL AS STRING) AS cause_source_value,
CAST(NULL AS integer) AS cause_source_concept_id  WHERE 1 = 0;"))

  cli::cli_alert_success("Created death table")
}
create_note <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  if (cdm_version == "5.3") {
    DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON KEY (person_id)
{create_sql(overwrite)} {schema_prefix(schema_name)}note
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS note_id,
CAST(NULL AS integer) AS person_id,
CAST(NULL AS date) AS note_date,
CAST(NULL AS TIMESTAMP) AS note_datetime,
CAST(NULL AS integer) AS note_type_concept_id,
CAST(NULL AS integer) AS note_class_concept_id,
CAST(NULL AS STRING) AS note_title,
CAST(NULL AS STRING) AS note_text,
CAST(NULL AS integer) AS encoding_concept_id,
CAST(NULL AS integer) AS language_concept_id,
CAST(NULL AS integer) AS provider_id,
CAST(NULL AS integer) AS visit_occurrence_id,
CAST(NULL AS integer) AS visit_detail_id,
CAST(NULL AS STRING) AS note_source_value  WHERE 1 = 0;"))
  } else {
    DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON KEY (person_id)
{create_sql(overwrite)} {schema_prefix(schema_name)}note
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS note_id,
CAST(NULL AS {id_int_type(id_bigint)}) AS person_id,
CAST(NULL AS date) AS note_date,
CAST(NULL AS TIMESTAMP) AS note_datetime,
CAST(NULL AS integer) AS note_type_concept_id,
CAST(NULL AS integer) AS note_class_concept_id,
CAST(NULL AS STRING) AS note_title,
CAST(NULL AS STRING) AS note_text,
CAST(NULL AS integer) AS encoding_concept_id,
CAST(NULL AS integer) AS language_concept_id,
CAST(NULL AS integer) AS provider_id,
CAST(NULL AS integer) AS visit_occurrence_id,
CAST(NULL AS integer) AS visit_detail_id,
CAST(NULL AS STRING) AS note_source_value,
CAST(NULL AS integer) AS note_event_id,
CAST(NULL AS integer) AS note_event_field_concept_id  WHERE 1 = 0;"))
  }


  cli::cli_alert_success("Created note table")
}
create_note_nlp <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON RANDOM
{create_sql(overwrite)} {schema_prefix(schema_name)}note_nlp
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS note_nlp_id,
CAST(NULL AS integer) AS note_id,
CAST(NULL AS integer) AS section_concept_id,
CAST(NULL AS STRING) AS snippet,
CAST(NULL AS STRING) AS `offset`,
CAST(NULL AS STRING) AS lexical_variant,
CAST(NULL AS integer) AS note_nlp_concept_id,
CAST(NULL AS integer) AS note_nlp_source_concept_id,
CAST(NULL AS STRING) AS nlp_system,
CAST(NULL AS date) AS nlp_date,
CAST(NULL AS TIMESTAMP) AS nlp_datetime,
CAST(NULL AS STRING) AS term_exists,
CAST(NULL AS STRING) AS term_temporal,
CAST(NULL AS STRING) AS term_modifiers  WHERE 1 = 0;"))

  cli::cli_alert_success("Created note nlp table")
}
create_specimen <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON KEY (person_id)
{create_sql(overwrite)} {schema_prefix(schema_name)}specimen
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS specimen_id,
CAST(NULL AS {id_int_type(id_bigint)}) AS person_id,
CAST(NULL AS integer) AS specimen_concept_id,
CAST(NULL AS integer) AS specimen_type_concept_id,
CAST(NULL AS date) AS specimen_date,
CAST(NULL AS TIMESTAMP) AS specimen_datetime,
CAST(NULL AS float) AS quantity,
CAST(NULL AS integer) AS unit_concept_id,
CAST(NULL AS integer) AS anatomic_site_concept_id,
CAST(NULL AS integer) AS disease_status_concept_id,
CAST(NULL AS STRING) AS specimen_source_id,
CAST(NULL AS STRING) AS specimen_source_value,
CAST(NULL AS STRING) AS unit_source_value,
CAST(NULL AS STRING) AS anatomic_site_source_value,
CAST(NULL AS STRING) AS disease_status_source_value  WHERE 1 = 0;"))

  cli::cli_alert_success("Created specimen table")
}
create_fact_relationship <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON RANDOM
{create_sql(overwrite)} {schema_prefix(schema_name)}fact_relationship
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS domain_concept_id_1,
CAST(NULL AS integer) AS fact_id_1,
CAST(NULL AS integer) AS domain_concept_id_2,
CAST(NULL AS integer) AS fact_id_2,
CAST(NULL AS integer) AS relationship_concept_id  WHERE 1 = 0;"))

  cli::cli_alert_success("Created fact relationship table")
}
create_location <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  if (cdm_version == "5.3") {
    DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON RANDOM
{create_sql(overwrite)} {schema_prefix(schema_name)}location
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS location_id,
CAST(NULL AS STRING) AS address_1,
CAST(NULL AS STRING) AS address_2,
CAST(NULL AS STRING) AS city,
CAST(NULL AS STRING) AS state,
CAST(NULL AS STRING) AS zip,
CAST(NULL AS STRING) AS county,
CAST(NULL AS STRING) AS location_source_value  WHERE 1 = 0;"))
  } else {
    DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON RANDOM
{create_sql(overwrite)} {schema_prefix(schema_name)}location
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS location_id,
CAST(NULL AS STRING) AS address_1,
CAST(NULL AS STRING) AS address_2,
CAST(NULL AS STRING) AS city,
CAST(NULL AS STRING) AS state,
CAST(NULL AS STRING) AS zip,
CAST(NULL AS STRING) AS county,
CAST(NULL AS STRING) AS location_source_value,
CAST(NULL AS integer) AS country_concept_id,
CAST(NULL AS STRING) AS country_source_value,
CAST(NULL AS float) AS latitude,
CAST(NULL AS float) AS longitude  WHERE 1 = 0;"))
  }


  cli::cli_alert_success("Created location table")
}
create_care_site <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON RANDOM
{create_sql(overwrite)} {schema_prefix(schema_name)}care_site
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS care_site_id,
CAST(NULL AS STRING) AS care_site_name,
CAST(NULL AS integer) AS place_of_service_concept_id,
CAST(NULL AS integer) AS location_id,
CAST(NULL AS STRING) AS care_site_source_value,
CAST(NULL AS STRING) AS place_of_service_source_value  WHERE 1 = 0;"))

  cli::cli_alert_success("Created care site table")
}
create_provider <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON RANDOM
{create_sql(overwrite)} {schema_prefix(schema_name)}provider
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS provider_id,
CAST(NULL AS STRING) AS provider_name,
CAST(NULL AS STRING) AS npi,
CAST(NULL AS STRING) AS dea,
CAST(NULL AS integer) AS specialty_concept_id,
CAST(NULL AS integer) AS care_site_id,
CAST(NULL AS integer) AS year_of_birth,
CAST(NULL AS integer) AS gender_concept_id,
CAST(NULL AS STRING) AS provider_source_value,
CAST(NULL AS STRING) AS specialty_source_value,
CAST(NULL AS integer) AS specialty_source_concept_id,
CAST(NULL AS STRING) AS gender_source_value,
CAST(NULL AS integer) AS gender_source_concept_id  WHERE 1 = 0;"))

  cli::cli_alert_success("Created provider table")
}
create_payer_plan_period <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON KEY (person_id)
{create_sql(overwrite)} {schema_prefix(schema_name)}payer_plan_period
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS payer_plan_period_id,
CAST(NULL AS {id_int_type(id_bigint)}) AS person_id,
CAST(NULL AS date) AS payer_plan_period_start_date,
CAST(NULL AS date) AS payer_plan_period_end_date,
CAST(NULL AS integer) AS payer_concept_id,
CAST(NULL AS STRING) AS payer_source_value,
CAST(NULL AS integer) AS payer_source_concept_id,
CAST(NULL AS integer) AS plan_concept_id,
CAST(NULL AS STRING) AS plan_source_value,
CAST(NULL AS integer) AS plan_source_concept_id,
CAST(NULL AS integer) AS sponsor_concept_id,
CAST(NULL AS STRING) AS sponsor_source_value,
CAST(NULL AS integer) AS sponsor_source_concept_id,
CAST(NULL AS STRING) AS family_source_value,
CAST(NULL AS integer) AS stop_reason_concept_id,
CAST(NULL AS STRING) AS stop_reason_source_value,
CAST(NULL AS integer) AS stop_reason_source_concept_id  WHERE 1 = 0;"))

  cli::cli_alert_success("Created payer plan period table")
}
create_cost <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON RANDOM
{create_sql(overwrite)} {schema_prefix(schema_name)}cost
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS cost_id,
CAST(NULL AS integer) AS cost_event_id,
CAST(NULL AS STRING) AS cost_domain_id,
CAST(NULL AS integer) AS cost_type_concept_id,
CAST(NULL AS integer) AS currency_concept_id,
CAST(NULL AS float) AS total_charge,
CAST(NULL AS float) AS total_cost,
CAST(NULL AS float) AS total_paid,
CAST(NULL AS float) AS paid_by_payer,
CAST(NULL AS float) AS paid_by_patient,
CAST(NULL AS float) AS paid_patient_copay,
CAST(NULL AS float) AS paid_patient_coinsurance,
CAST(NULL AS float) AS paid_patient_deductible,
CAST(NULL AS float) AS paid_by_primary,
CAST(NULL AS float) AS paid_ingredient_cost,
CAST(NULL AS float) AS paid_dispensing_fee,
CAST(NULL AS integer) AS payer_plan_period_id,
CAST(NULL AS float) AS amount_allowed,
CAST(NULL AS integer) AS revenue_code_concept_id,
CAST(NULL AS STRING) AS revenue_code_source_value,
CAST(NULL AS integer) AS drg_concept_id,
CAST(NULL AS STRING) AS drg_source_value  WHERE 1 = 0;"))

  cli::cli_alert_success("Created cost table")
}
create_drug_era <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON KEY (person_id)
{create_sql(overwrite)} {schema_prefix(schema_name)}drug_era
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS drug_era_id,
CAST(NULL AS {id_int_type(id_bigint)}) AS person_id,
CAST(NULL AS integer) AS drug_concept_id,
CAST(NULL AS date) AS drug_era_start_date,
CAST(NULL AS date) AS drug_era_end_date,
CAST(NULL AS integer) AS drug_exposure_count,
CAST(NULL AS integer) AS gap_days  WHERE 1 = 0;"))

  cli::cli_alert_success("Created drug era table")
}
create_dose_era <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON KEY (person_id)
{create_sql(overwrite)} {schema_prefix(schema_name)}dose_era
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS dose_era_id,
CAST(NULL AS {id_int_type(id_bigint)}) AS person_id,
CAST(NULL AS integer) AS drug_concept_id,
CAST(NULL AS integer) AS unit_concept_id,
CAST(NULL AS float) AS dose_value,
CAST(NULL AS date) AS dose_era_start_date,
CAST(NULL AS date) AS dose_era_end_date  WHERE 1 = 0;"))

  cli::cli_alert_success("Created dose era table")
}
create_condition_era <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON KEY (person_id)
{create_sql(overwrite)} {schema_prefix(schema_name)}condition_era
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS condition_era_id,
CAST(NULL AS {id_int_type(id_bigint)}) AS person_id,
CAST(NULL AS integer) AS condition_concept_id,
CAST(NULL AS date) AS condition_era_start_date,
CAST(NULL AS date) AS condition_era_end_date,
CAST(NULL AS integer) AS condition_occurrence_count  WHERE 1 = 0;"))

  cli::cli_alert_success("Created condition era table")
}
create_episode <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON KEY (person_id)
{create_sql(overwrite)} {schema_prefix(schema_name)}episode
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS episode_id,
CAST(NULL AS {id_int_type(id_bigint)}) AS person_id,
CAST(NULL AS integer) AS episode_concept_id,
CAST(NULL AS date) AS episode_start_date,
CAST(NULL AS TIMESTAMP) AS episode_start_datetime,
CAST(NULL AS date) AS episode_end_date,
CAST(NULL AS TIMESTAMP) AS episode_end_datetime,
CAST(NULL AS integer) AS episode_parent_id,
CAST(NULL AS integer) AS episode_number,
CAST(NULL AS integer) AS episode_object_concept_id,
CAST(NULL AS integer) AS episode_type_concept_id,
CAST(NULL AS STRING) AS episode_source_value,
CAST(NULL AS integer) AS episode_source_concept_id  WHERE 1 = 0;"))

  cli::cli_alert_success("Created episode table")
}
create_episode_event <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  if (cdm_version == "5.3") {
    return(NULL)
  } else {
    DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON RANDOM
{create_sql(overwrite)} {schema_prefix(schema_name)}episode_event
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS episode_id,
CAST(NULL AS integer) AS event_id,
CAST(NULL AS integer) AS episode_event_field_concept_id  WHERE 1 = 0;"))

    cli::cli_alert_success("Created episode event table")
  }
}
create_metadata <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  if (cdm_version == "5.3") {
    DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON RANDOM
{create_sql(overwrite)} {schema_prefix(schema_name)}metadata
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS metadata_concept_id,
CAST(NULL AS integer) AS metadata_type_concept_id,
CAST(NULL AS STRING) AS name,
CAST(NULL AS STRING) AS value_as_string,
CAST(NULL AS integer) AS value_as_concept_id,
CAST(NULL AS date) AS metadata_date,
CAST(NULL AS TIMESTAMP) AS metadata_datetime  WHERE 1 = 0;"))
  } else {
    DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON RANDOM
{create_sql(overwrite)} {schema_prefix(schema_name)}metadata
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS metadata_id,
CAST(NULL AS integer) AS metadata_concept_id,
CAST(NULL AS integer) AS metadata_type_concept_id,
CAST(NULL AS STRING) AS name,
CAST(NULL AS STRING) AS value_as_string,
CAST(NULL AS integer) AS value_as_concept_id,
CAST(NULL AS float) AS value_as_number,
CAST(NULL AS date) AS metadata_date,
CAST(NULL AS TIMESTAMP) AS metadata_datetime  WHERE 1 = 0;"))
  }


  cli::cli_alert_success("Created metadata table")
}
create_cdm_source <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  if (cdm_version == "5.3") {
    DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON RANDOM
{create_sql(overwrite)} {schema_prefix(schema_name)}cdm_source
USING DELTA
AS
SELECT
CAST(NULL AS STRING) AS cdm_source_name,
CAST(NULL AS STRING) AS cdm_source_abbreviation,
CAST(NULL AS STRING) AS cdm_holder,
CAST(NULL AS STRING) AS source_description,
CAST(NULL AS STRING) AS source_documentation_reference,
CAST(NULL AS STRING) AS cdm_etl_reference,
CAST(NULL AS date) AS source_release_date,
CAST(NULL AS date) AS cdm_release_date,
CAST(NULL AS STRING) AS cdm_version,
CAST(NULL AS STRING) AS vocabulary_version  WHERE 1 = 0;"))
  } else {
    DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON RANDOM
{create_sql(overwrite)} {schema_prefix(schema_name)}cdm_source
USING DELTA
AS
SELECT
CAST(NULL AS STRING) AS cdm_source_name,
CAST(NULL AS STRING) AS cdm_source_abbreviation,
CAST(NULL AS STRING) AS cdm_holder,
CAST(NULL AS STRING) AS source_description,
CAST(NULL AS STRING) AS source_documentation_reference,
CAST(NULL AS STRING) AS cdm_etl_reference,
CAST(NULL AS date) AS source_release_date,
CAST(NULL AS date) AS cdm_release_date,
CAST(NULL AS STRING) AS cdm_version,
CAST(NULL AS integer) AS cdm_version_concept_id,
CAST(NULL AS STRING) AS vocabulary_version  WHERE 1 = 0;"))
  }

  cli::cli_alert_success("Created cdm source table")
}
create_concept <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON RANDOM
{create_sql(overwrite)} {schema_prefix(schema_name)}concept
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS concept_id,
CAST(NULL AS STRING) AS concept_name,
CAST(NULL AS STRING) AS domain_id,
CAST(NULL AS STRING) AS vocabulary_id,
CAST(NULL AS STRING) AS concept_class_id,
CAST(NULL AS STRING) AS standard_concept,
CAST(NULL AS STRING) AS concept_code,
CAST(NULL AS date) AS valid_start_date,
CAST(NULL AS date) AS valid_end_date,
CAST(NULL AS STRING) AS invalid_reason  WHERE 1 = 0;"))

  cli::cli_alert_success("Created concept table")
}
create_vocabulary <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON RANDOM
{create_sql(overwrite)} {schema_prefix(schema_name)}vocabulary
USING DELTA
AS
SELECT
CAST(NULL AS STRING) AS vocabulary_id,
CAST(NULL AS STRING) AS vocabulary_name,
CAST(NULL AS STRING) AS vocabulary_reference,
CAST(NULL AS STRING) AS vocabulary_version,
CAST(NULL AS integer) AS vocabulary_concept_id  WHERE 1 = 0;"))

  cli::cli_alert_success("Created vocabulary table")
}
create_domain <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON RANDOM
{create_sql(overwrite)} {schema_prefix(schema_name)}domain
USING DELTA
AS
SELECT
CAST(NULL AS STRING) AS domain_id,
CAST(NULL AS STRING) AS domain_name,
CAST(NULL AS integer) AS domain_concept_id  WHERE 1 = 0;"))

  cli::cli_alert_success("Created domain table")
}
create_concept_class <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON RANDOM
{create_sql(overwrite)} {schema_prefix(schema_name)}concept_class
USING DELTA
AS
SELECT
CAST(NULL AS STRING) AS concept_class_id,
CAST(NULL AS STRING) AS concept_class_name,
CAST(NULL AS integer) AS concept_class_concept_id  WHERE 1 = 0;"))

  cli::cli_alert_success("Created concept class table")
}
create_concept_relationship <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON RANDOM
{create_sql(overwrite)} {schema_prefix(schema_name)}concept_relationship
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS concept_id_1,
CAST(NULL AS integer) AS concept_id_2,
CAST(NULL AS STRING) AS relationship_id,
CAST(NULL AS date) AS valid_start_date,
CAST(NULL AS date) AS valid_end_date,
CAST(NULL AS STRING) AS invalid_reason  WHERE 1 = 0;"))

  cli::cli_alert_success("Created concept relationship table")
}
create_relationship <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON RANDOM
{create_sql(overwrite)} {schema_prefix(schema_name)}relationship
USING DELTA
AS
SELECT
CAST(NULL AS STRING) AS relationship_id,
CAST(NULL AS STRING) AS relationship_name,
CAST(NULL AS STRING) AS is_hierarchical,
CAST(NULL AS STRING) AS defines_ancestry,
CAST(NULL AS STRING) AS reverse_relationship_id,
CAST(NULL AS integer) AS relationship_concept_id  WHERE 1 = 0;"))

  cli::cli_alert_success("Created relationship table")
}
create_concept_synonym <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON RANDOM
{create_sql(overwrite)} {schema_prefix(schema_name)}concept_synonym
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS concept_id,
CAST(NULL AS STRING) AS concept_synonym_name,
CAST(NULL AS integer) AS language_concept_id  WHERE 1 = 0;"))

  cli::cli_alert_success("Created concept synonym table")
}
create_concept_ancestor <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON RANDOM
{create_sql(overwrite)} {schema_prefix(schema_name)}concept_ancestor
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS ancestor_concept_id,
CAST(NULL AS integer) AS descendant_concept_id,
CAST(NULL AS integer) AS min_levels_of_separation,
CAST(NULL AS integer) AS max_levels_of_separation  WHERE 1 = 0;"))

  cli::cli_alert_success("Created concept ancestor table")
}
create_source_to_concept_map <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON RANDOM
{create_sql(overwrite)} {schema_prefix(schema_name)}source_to_concept_map
USING DELTA
AS
SELECT
CAST(NULL AS STRING) AS source_code,
CAST(NULL AS integer) AS source_concept_id,
CAST(NULL AS STRING) AS source_vocabulary_id,
CAST(NULL AS STRING) AS source_code_description,
CAST(NULL AS integer) AS target_concept_id,
CAST(NULL AS STRING) AS target_vocabulary_id,
CAST(NULL AS date) AS valid_start_date,
CAST(NULL AS date) AS valid_end_date,
CAST(NULL AS STRING) AS invalid_reason  WHERE 1 = 0;"))

  cli::cli_alert_success("Created source to concept map table")
}
create_drug_strength <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON RANDOM
{create_sql(overwrite)} {schema_prefix(schema_name)}drug_strength
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS drug_concept_id,
CAST(NULL AS integer) AS ingredient_concept_id,
CAST(NULL AS float) AS amount_value,
CAST(NULL AS integer) AS amount_unit_concept_id,
CAST(NULL AS float) AS numerator_value,
CAST(NULL AS integer) AS numerator_unit_concept_id,
CAST(NULL AS float) AS denominator_value,
CAST(NULL AS integer) AS denominator_unit_concept_id,
CAST(NULL AS integer) AS box_size,
CAST(NULL AS date) AS valid_start_date,
CAST(NULL AS date) AS valid_end_date,
CAST(NULL AS STRING) AS invalid_reason  WHERE 1 = 0;"))

  cli::cli_alert_success("Created drug strength table")
}
create_cohort <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON RANDOM
{create_sql(overwrite)} {schema_prefix(schema_name)}cohort
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS cohort_definition_id,
CAST(NULL AS integer) AS subject_id,
CAST(NULL AS date) AS cohort_start_date,
CAST(NULL AS date) AS cohort_end_date  WHERE 1 = 0;"))

  cli::cli_alert_success("Created cohort table")
}
create_cohort_definition <- function(con, schema_name, cdm_version, overwrite, id_bigint) {
  DBI::dbExecute(con, glue::glue("-- DISTRIBUTE ON RANDOM
{create_sql(overwrite)} {schema_prefix(schema_name)}cohort_definition
USING DELTA
AS
SELECT
CAST(NULL AS integer) AS cohort_definition_id,
CAST(NULL AS STRING) AS cohort_definition_name,
CAST(NULL AS STRING) AS cohort_definition_description,
CAST(NULL AS integer) AS definition_type_concept_id,
CAST(NULL AS STRING) AS cohort_definition_syntax,
CAST(NULL AS integer) AS subject_concept_id,
CAST(NULL AS date) AS cohort_initiation_date  WHERE 1 = 0;"))

  cli::cli_alert_success("Created cohort definition table")
}
