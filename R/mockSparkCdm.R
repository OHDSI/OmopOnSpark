#' creates a local mock database connection
#'
#' @param con A spark connection using `sparklyr::spark_connect()`.
#' @param databaseName Name of the dataset to download.
#' @param cdmSchema Schema to include omop standard tables. Schema will be
#' created if it does not exist.
#' @param writeSchema Schema to include cohort tables. Schema will be created if
#' it does not exist.
#'
#' @return Connection to the spark data set with Eunomia loaded.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' con <- mockSparkCon()
#' }
#'
mockSparkCon <- function(con = sparklyr::spark_connect(master = "local"),
                         databaseName = "GiBleed",
                         cdmSchema = list(schema = "default"),
                         writeSchema = list(schema = "default")) {
  # initial validation
  con <- validateConnection(con)
  omopgenerics::assertCharacter(databaseName, length = 1)
  cdmSchema <- validateSchema(cdmSchema)
  writeSchema <- validateSchema(writeSchema)

  # create schemas if necessary
  createSchema(con, cdmSchema)
  createSchema(con, writeSchema)

  # download dataset in temp file
  url <- paste0("https://example-data.ohdsi.dev/", databaseName, ".zip")
  folder <- file.path(tempdir(), paste0("dataset_", paste0(sample(letters, 6), collapse = "")))
  dir.create(folder)
  zipFile <- file.path(folder, "dataset.zip")
  utils::download.file(url, zipFile)

  # unzip dataset
  utils::unzip(zipFile, exdir = folder)

  list.files(file.path(folder, databaseName), full.names = TRUE) |>
    purrr::map(\(x) {
      name <- tools::file_path_sans_ext(basename(x))
      cli::cli_inform("Creating table: {.pkg {name}}")
      sparklyr::spark_read_parquet(
        sc = con,
        name = name,
        path = x,
        overwrite = TRUE
      )
      if ("schema" %in% names(cdmSchema)) {
        DBI::dbExecute(conn = con, glue::glue(
          "CREATE TABLE {quoteName(cdmSchema, name)} AS (SELECT * FROM `{name}`)"
        ))
        DBI::dbExecute(conn = con, glue::glue("DROP TABLE `{name}`"))
      }
    })

  unlink(folder, recursive = TRUE)

  return(con)
}

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
