
#' Create a spark source object.
#'
#' @param con A connection to a spark database.
#' @param writeSchema A write schema with writing permissions.
#' @param tempSchema A write schema with writing permissions to emulate
#' temporary tables.
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
sparkSource <- function(con, writeSchema, tempSchema = writeSchema, logSql = NULL) {
  con <- validateConnection(con)
  writeSchema <- validateSchema(writeSchema, FALSE)
  tempSchema <- validateSchema(tempSchema, FALSE)
  logSql <- validateLogSql(logSql)

  # create source
  newSparkSource(
    con = con,
    writeSchema = writeSchema,
    tempSchema = tempSchema,
    logSql = logSql
  )
}

newSparkSource <- function(con, writeSchema, tempSchema, logSql) {
  tempPrefix <- paste0("temp_", paste0(sample(letters, 5), collapse = ""), "_")
  tempSchema$prefix <- tempPrefix
  structure(
    .Data = list(),
    con = con,
    write_schema = writeSchema,
    log_sql = logSql,
    temp_schema = tempSchema,
    class = "spark_cdm"
  ) |>
    omopgenerics::newCdmSource(sourceType = "sparklyr")
}

#' @export
insertTable.spark_cdm <- function(cdm,
                                  name,
                                  table,
                                  overwrite = TRUE,
                                  temporary = FALSE) {
  # get attributes
  schema <- schemaToWrite(cdm, temporary)
  con <- getCon(cdm)

  # check overwrite
  if (overwrite) {
    sparkDropTable(con = con, schema = schema, name = name)
  } else if (name %in% sparkListTables(con = con, schema = schema)) {
    cli::cli_abort(c(
      x = "Table {.pkg {name}} already exists use `overwrite = FALSE`."
    ))
  }

  # write table
  sparkWriteTable(con = con, schema = schema, name = name, value = table)

  # read table
  sparkReadTable(con = con, schema = schema, name = name) |>
    omopgenerics::newCdmTable(src = cdm, name = name)
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
  schema <- writeSchema(src)

  remoteName <-  dbplyr::remote_name(value)
  if ("prefix" %in% names(schema)) {
    prefix <- schema$prefix
    if (substr(remoteName, 1, nchar(prefix)) == prefix) {
      remoteName <- substr(remoteName, nchar(prefix) + 1, nchar(remoteName))
    } else {
      cli::cli_warn(c("!" = "The {.var {remoteName}} does have the required prefix."))
      cli::cli_inform(c(i = "Creating a copy in `writeSchema`"))
      value <- sparkComputeTable(query = value, schema = schema, name = remoteName)
    }
  }

  omopgenerics::newCdmTable(table = value, src = src, name = remoteName)
}

#' @export
listSourceTables.spark_cdm <- function(cdm) {
  sparkListTables(con = getCon(cdm), schema = writeSchema(cdm))
}

#' @export
dropSourceTable.spark_cdm <- function(cdm, name) {
  sparkDropTable(con = getCon(cdm), schema = writeSchema(cdm), name = name)
}

#' @export
readSourceTable.spark_cdm <- function(cdm, name) {
  sparkReadTable(con = getCon(cdm), schema = writeSchema(cdm), name = name)
}

#' @export
cdmDisconnect.spark_cdm <- function(cdm, dropWriteSchema = FALSE, dropTempSchema = TRUE, ...) {
  # input check
  omopgenerics::assertLogical(dropWriteSchema, length = 1)
  omopgenerics::assertLogical(dropTempSchema, length = 1)

  # connection
  con <- getCon(cdm)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # drop tables if needed
  if (dropWriteSchema) {
    schema <- writeSchema(src = cdm)
    nms <- sparkListTables(con = con, schema = schema)
    purrr::map(nms, \(x) sparkDropTable(con = con, schema = schema, name = x))
  }

  # drop temp tables if needed
  if (dropTempSchema) {
    schema <- tempSchema(src = cdm)
    nms <- sparkListTables(con = con, schema = schema)
    purrr::map(nms, \(x) sparkDropTable(con = con, schema = schema, name = x))
  }

  return(invisible(TRUE))
}

#' @export
insertCdmTo.spark_cdm <- function(cdm, to) {
  con <- getCon(to)
  writeSchema <- writeSchema(to)

  achillesSchema <- NULL
  cohorts <- character()
  other <- character()
  for (nm in names(cdm)) {
    x <- dplyr::collect(cdm[[nm]])
    cl <- class(x)
    if ("achilles_table" %in% cl) {
      achilles <- writeSchema
    }
    if (!any(c("achilles_table", "omop_table", "cohort_table") %in% cl)) {
      other <- c(other, nm)
    }
    insertTable(cdm = to, name = nm, table = x, overwrite = TRUE)
    if ("cohort_table" %in% cl) {
      cohorts <- c(cohorts, nm)
      insertTable(cdm = to, name = paste0(nm, "_set"), table = attr(x, "cohort_set"), overwrite = TRUE)
      insertTable(cdm = to, name = paste0(nm, "_attrition"), table = attr(x, "cohort_attrition"), overwrite = TRUE)
      insertTable(cdm = to, name = paste0(nm, "_codelist"), table = attr(x, "cohort_codelist"), overwrite = TRUE)
    }
  }

  newCdm <- cdmFromSpark(
    con = con,
    cdmSchema = writeSchema,
    writeSchema = writeSchema,
    achillesSchema = achillesSchema,
    cohortTables = cohorts,
    cdmVersion = omopgenerics::cdmVersion(cdm),
    cdmName = omopgenerics::cdmName(cdm),
    .softValidation = TRUE,
    logSql = logSql(to)
  )

  newCdm <- omopgenerics::readSourceTable(cdm = newCdm, name = other)

  return(newCdm)
}

# internal functions
tempSchema <- function(src) {
  attr(src, "temp_schema")
}
writeSchema <- function(src) {
  attr(src, "write_schema")
}
getCon <- function(src) {
  attr(src, "con")
}
schemaToWrite <- function(src, temporary) {
  if (temporary) tempSchema(src) else writeSchema(src)
}
logSql <- function(src) {
  attr(src, "log_sql")
}

sparkListTables <- function(con, schema) {
  schemaName <- paste0(c(schema$catalog, schema$schema), collapse = ".")
  x <- DBI::dbGetQuery(con, glue::glue("SHOW TABLES IN {schemaName}")) |>
    dplyr::as_tibble() |>
    dplyr::filter(!.data$isTemporary) |>
    dplyr::pull("tableName")
  if (length(schema$prefix) > 0) {
    x <- x[startsWith(x = x, prefix = schema$prefix)]
    x <- substr(x, nchar(schema$prefix) + 1, nchar(x))
  }
  x <- x[nchar(x) > 0]
  return(x)
}
sparkDropTable <- function(con, schema, name) {
  DBI::dbExecute(con, glue::glue("DROP TABLE IF EXISTS {fullName(schema, name)}"))
  invisible(TRUE)
}
sparkReadTable <- function(con, schema, name) {
  dplyr::tbl(con, fullName(schema, name))
}
sparkWriteTable <- function(con, schema, name, value) {

  sparkDropTable(con = con, schema = schema, name = name)

  # it take into account catalog, schema and prefix
  fullname <- fullName(schema, name)
  # insert data
  DBI::dbWriteTable(conn = con, name = fullname, value = value, overwrite = TRUE)
}
sparkComputeTable <- function(query, schema, name) {
  sparklyr::spark_write_table(
    x = query, name = fullName(schema, name), mode = "overwrite"
  )
}
