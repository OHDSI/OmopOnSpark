#' creates a local mock database connection
#'
#' @param con A spark connection using `sparklyr::spark_connect()`.
#' @param databaseName Name of the dataset to download.
#' @param cdmSchema Schema to include omop standard tables. Schema will be
#' created if it does not exist.
#' @param writeSchema Schema to include cohort tables. Schema will be created if
#' it does not exist.
#' @param tempSchema Schema to include temporary tables.
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
mockSparkCdm <- function(con = sparklyr::spark_connect(master = "local"),
                         databaseName = "GiBleed",
                         cdmSchema = list(schema = "default"),
                         writeSchema = list(schema = "default"),
                         tempSchema = writeSchema) {
  # initial validation
  con <- validateConnection(con)
  omopgenerics::assertCharacter(databaseName, length = 1)
  cdmSchema <- validateSchema(cdmSchema, FALSE)
  writeSchema <- validateSchema(writeSchema, FALSE)
  tempSchema <- validateSchema(tempSchema, FALSE)

  # create schemas if necessary
  createSchema(con, cdmSchema)
  createSchema(con, writeSchema)
  createSchema(con, tempSchema)

  # download dataset in temp file
  url <- paste0("https://example-data.ohdsi.dev/", databaseName, ".zip")
  folder <- file.path(tempdir(), paste0("dataset_", paste0(sample(letters, 6), collapse = "")))
  dir.create(folder)
  on.exit(unlink(folder, recursive = TRUE))
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
          "CREATE TABLE {fullName(cdmSchema, name)} AS (SELECT * FROM `{name}`)"
        ))
        DBI::dbExecute(conn = con, glue::glue("DROP TABLE `{name}`"))
      }
    })

  cdm <- cdmFromSpark(
    con = con, cdmSchema = cdmSchema, writeSchema = writeSchema,
    tempSchema = tempSchema, cdmName = databaseName, .softValidation = TRUE
  )

  return(cdm)
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
