#' creates a cdm reference to local spark OMOP CDM tables
#'
#' @param path A directory for files
#'
#' @return A cdm reference with synthetic data in a local spark connection
#'
#' @export
#'
#' @examples
#' \dontrun{
#' mockSparkCdm()
#' }
#'
mockSparkCdm <- function(path) {

  folder <- path
  working_config <- sparklyr::spark_config()
  working_config$spark.sql.warehouse.dir <- folder
  con <- sparklyr::spark_connect(master = "local",
                                 config = working_config)
  createSchema(con = con,
               schema = list(schema = "omop"))
  src <- sparkSource(con = con,
                     writeSchema = list(schema = "omop"))

  cdm_local <- omock::mockCdmReference() |>
    omock::mockPerson(nPerson = 10) |>
    omock::mockObservationPeriod() |>
    omock::mockConditionOccurrence() |>
    omock::mockCohort()

  folder <- file.path(tempdir(), "temp_spark")
  working_config <- sparklyr::spark_config()
  working_config$spark.sql.warehouse.dir <- folder
  con <- sparklyr::spark_connect(master = "local", config = working_config)
  createSchema(con = con, schema = list(schema = "omop"))
  src <- sparkSource(con = con,
                     writeSchema = list(schema = "omop"))
  cdm <- insertCdmTo(cdm_local, src)

  cdm <- cdmFromSpark(
    con = con,
    cdmSchema = "omop",
    writeSchema = "omop",
    # tempSchema = "temp",
    cdmName = "mock local spark",
    .softValidation = TRUE
  )

  return(cdm)
}

#' Create schema
#'
#' @param con con
#' @param schema schema
#'
#' @returns con
#' @export
createSchema <- function(con, schema) {
  nms <- names(schema)
  if ("catalog" %in% nms) {
    cli::cli_abort("this function does not support catalogs for the moment.")
  }
  if ("schema" %in% nms) {
    DBI::dbExecute(con, glue::glue("CREATE SCHEMA IF NOT EXISTS {schema$schema}"))
  }
  return(invisible())
}
