test_that("eunomia test", {

folder <- file.path(tempdir(), "temp_eunomia")
working_config <- sparklyr::spark_config()
working_config$spark.sql.warehouse.dir <- folder
con <- sparklyr::spark_connect(master = "local", config = working_config)
DBI::dbExecute(con, glue::glue("CREATE SCHEMA IF NOT EXISTS omop"))
DBI::dbExecute(con, glue::glue("CREATE SCHEMA IF NOT EXISTS results"))
src <- sparkSource(con = con,cdmSchema = "omop", writeSchema = "results")

con_eunomia <- DBI::dbConnect(duckdb::duckdb(), CDMConnector::eunomiaDir())
cdm_eunomia_duckdb <- CDMConnector::cdmFromCon(con_eunomia,
                                        cdmSchema = "main",
                                        writeSchema = "main")
cdm_eunomia_local <- cdm_eunomia_duckdb |> dplyr::collect()
cdm_eunomia_spark <- insertCdmTo(cdm = cdm_eunomia_local, to = src)

# compare local duckdb vs local spark
duckdb_bench <- CDMConnector::benchmarkCDMConnector(cdm_eunomia_duckdb)
spark_bench <- CDMConnector::benchmarkCDMConnector(cdm_eunomia_spark)
cdm_bench <- dplyr::bind_rows(duckdb_bench, spark_bench)
cdm_bench |>
  ggplot2::ggplot() +
  ggplot2::facet_wrap(ggplot2::vars(task)) +
  ggplot2::geom_col(ggplot2::aes(dbms, time_taken_secs))

duckdb_pp_bench <- PatientProfiles::benchmarkPatientProfiles(cdm_eunomia_duckdb)
# spark_pp_bench <- PatientProfiles::benchmarkPatientProfiles(cdm_eunomia_spark)
# omopgenerics::bind(duckdb_pp_bench, spark_pp_bench) |>
#   visOmopResults::visOmopTable(groupColumn = "task")

# duckdb_cohort_bench <- CohortConstructor::benchmarkCohortConstructor(cdm_eunomia_duckdb, runCIRCE = FALSE)
# spark_cohort_bench <- CohortConstructor::benchmarkCohortConstructor(cdm_eunomia_spark, runCIRCE = FALSE)

duckdb_inc_prev_bench <- IncidencePrevalence::benchmarkIncidencePrevalence(cdm_eunomia_duckdb)
# spark_inc_prev_bench <- IncidencePrevalence::benchmarkIncidencePrevalence(cdm_eunomia_spark)


cdmDisconnect(cdm_eunomia_duckdb)
cdmDisconnect(cdm_eunomia_spark)

})

