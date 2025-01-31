
#' Create a spark source object.
#'
#' @param con A connection to a spark database.
#' @param writeSchema A write schema with writing permissions. Use NULL to use
#' temp tables.
#' @param logSql Whether to log executed sql in a log file
#'
#' @return A spark_cdm object.
#' @export
#'
#' @examples
#' \dontrun{
#' con <- sparklyr::spark_connect(master = "local")
#' sparkSource(con)
#' }
#'
sparkSource <- function(con, writeSchema = NULL, logSql = NULL) {
  con <- validateConnection(con)
  writeSchema <- validateSchema(writeSchema)
  logSql <- validateLogSql(logSql)

  # create source
  newSparkSource(con = con, schema = writeSchema, logSql = logSql)
}

newSparkSource <- function(con, schema, logSql) {
  structure(
    .Data = list(),
    con = con,
    write_schema = schema,
    log_sql = logSql,
    class = "spark_cdm"
  ) |>
    omopgenerics::newCdmSource(sourceType = "sparklyr")
}

# methods

#' @export
insertTable.spark_cdm <- function(cdm,
                                  name,
                                  table,
                                  overwrite = TRUE,
                                  temporary = FALSE) {
  sparkWriteTable(con = attr(cdm, "con"), schema = attr(cdm, "write_schema"), name = name, value = table) |>
    omopgenerics::newCdmTable(src = cdm, name = name)
}

#' @export
compute.spark_cdm <- function(x, name, temporary = FALSE, overwrite = TRUE, ...) {
  # check source and name
  source <- attr(x, "tbl_source")
  con <- attr(source, "con")
  schema <- attr(source, "write_schema")

  x <- sparkComputeTable(query = x, schema = schema, name = name)

  class(x) <- c("db_cdm", class(x))
  return(x)
}

#' @export
cdmTableFromSource.spark_cdm <- function(src, value) {
  if (inherits(value, "data.frame")) {
    "To insert a local table to a cdm_reference object use insertTable function." |>
      cli::cli_abort()
  }
  if (!inherits(value, "tbl_lazy")) {
    cli::cli_abort(
      "Can't assign an object of class: {paste0(class(value), collapse = ", ")}
      to a spark cdm_reference object."
    )
  }
  schema <- attr(src, "write_schema")

  remoteName <- sparklyr::spark_table_name(value)
  if ("prefix" %in% names(schema)) {
    prefix <- schema$prefix
    if (substr(remoteName, 1, nchar(prefix)) == prefix) {
      remoteName <- substr(remoteName, nchar(prefix) + 1, nchar(remoteName))
    }
  }

  omopgenerics::newCdmTable(table = value, src =src, name = remoteName)
}

#' @export
listSourceTables.spark_cdm <- function(cdm) {
  sparkListTables(con = attr(cdm, "con"), schema = attr(cdm, "write_schema"))
}

#' @export
dropSourceTable.spark_cdm <- function(cdm, name) {
  sparkDropTable(con = attr(cdm, "con"), schema = attr(cdm, "write_schema"), name = name)
}

#' @export
readSourceTable.spark_cdm <- function(cdm, name) {
  sparkReadTable(con = attr(cdm, "con"), schema = attr(cdm, "write_schema"), name = name)
}

# internal functions
sparkListTables <- function(con, schema) {
  if (!any(c("catalog", "schema") %in% names(schema))) {
    x <- DBI::dbGetQuery(conn = con, "SHOW TABLES")
  } else {
    schemaName <- paste0(c(schema$catalog, schema$schema), collapse = ".")
    x <- DBI::dbGetQuery(con, glue::glue("SHOW TABLES IN {schemaName}"))
  }
  x <- x |>
    dplyr::as_tibble() |>
    dplyr::filter(.data$isTemporary != !!any(c("catalog", "schema") %in% names(schema))) |>
    dplyr::pull("tableName")
  if (length(schema$prefix) > 0) {
    x <- x |>
      purrr::keep(\(x) startsWith(x = x, prefix = schema$prefix)) |>
      purrr::map_chr(\(x) substr(x, nchar(schema$prefix) + 1, nchar(x)))
  }
  x <- x[nchar(x) > 0]
  return(x)
}
sparkDropTable <- function(con, schema, name) {
  DBI::dbExecute(con, glue::glue("DROP TABLE IF EXISTS {fullName(schema, name)}"))
  TRUE
}
sparkReadTable <- function(con, schema, name) {
  dplyr::tbl(con, fullName(schema, name))
}
sparkWriteTable <- function(con, schema, name, value) {
  # it take into account catalog, schema and prefix
  fullname <- fullName(schema, name)

  # check whether it is temporary table or permanent
  if (isTemporarySchema(schema)) {
    # insert temporary table
    x <- sparklyr::sdf_copy_to(con, value, name = fullname, overwrite = TRUE)
  } else {
    tmpName <- omopgenerics::uniqueTableName()
    # insert as temp table
    sparklyr::sdf_copy_to(con, value, name = tmpName, overwrite = TRUE) |>
      # copy to permanent schema
      sparklyr::spark_write_table(name = fullname, mode = "overwrite")
    # drop temp table
    sparkDropTable(con = con, schema = list(), name = tmpName)
    x <- sparkReadTable(con = con, schema = schema, name = name)
  }
  x
}
sparkComputeTable <- function(query, schema, name) {
  if (isTemporarySchema(schema)) {
    x <- sparklyr::sdf_register(x = query, name = name)
  } else {
    sparklyr::spark_write_table(query, name = fullName(schema, name), mode = "overwrite")
    con <- sparklyr::spark_connection(query)
    x <- sparkReadTable(con = con, schema = schema, name = name)
  }
  x
}
