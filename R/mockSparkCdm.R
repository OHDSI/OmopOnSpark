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
  list.files(path)

  sparklyr::spark_connect(
    master = "local",
    config = working_config,
    spark_home = Sys.getenv("SPARK_HOME")
  )
  working_config <- sparklyr::spark_config()
  working_config$spark.sql.warehouse.dir <- folder
  java_home_path <- Sys.getenv("JAVA_HOME")
  if(!is.null(java_home_path)){
    working_config$`sparklyr.shell.env` <- paste0("JAVA_HOME=", java_home_path)
  }
  # browser()
  sc <- sparklyr::spark_connect(
    master = "local",
    config = working_config,
    spark_home = Sys.getenv("SPARK_HOME")
  )

  src <- sparkSource(
    con = sc,
    cdmSchema = NULL,
    writeSchema = NULL,
    writePrefix = "my_study_"
  )

  cdm_local <- omock::mockCdmReference() |>
    omock::mockPerson(nPerson = 10) |>
    omock::mockObservationPeriod() |>
    omock::mockConditionOccurrence() |>
    omock::mockCohort()

  cdm <- insertCdmTo(cdm_local, src)

  cdm <- cdmFromSpark(
    con = sc,
    cdmSchema = NULL,
    writeSchema = NULL,
    cdmName = "mock local spark",
    .softValidation = TRUE,
    writePrefix = "my_study_"
  )

  return(cdm)
}

