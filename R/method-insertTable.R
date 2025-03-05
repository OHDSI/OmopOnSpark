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

sparkWriteTable <- function(con, schema, name, value) {

  sparkDropTable(con = con, schema = schema, name = name)

  # it take into account catalog, schema and prefix
  fullname <- fullName(schema, name)
  # insert data
  DBI::dbWriteTable(conn = con, name = fullname, value = value, overwrite = TRUE)
}

