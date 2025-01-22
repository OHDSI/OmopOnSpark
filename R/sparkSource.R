
#' Create a spark source object.
#'
#' @param con A connection to a spark database.
#' @param writeSchema A write schema with writing permissions. Use NULL to use
#' temp tables.
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
sparkSource <- function(con, writeSchema = NULL) {
  # validate connection
  con <- validateConnection(con)

  # validate writeSchema
  writeSchema <- validateSchema(writeSchema)
  # check writing permissions

  if (!any(c("catalog", "schema") %in% names(writeSchema))) {
    cli::cli_inform(c("!" = "This `writeSchema` does not allow permantent tables, temporary tables will be used even if `dplyr::compute(..., temporary = FALSE)` is used."))
  }

  # create source
  structure(
    .Data = list(), con = con, write_schema = writeSchema, class = "spark_cdm"
  ) |>
    omopgenerics::newCdmSource()
}

conFromSource <- function(src) {
  attr(src, "con")
}
writeSchemaFromSource <- function(src) {
  attr(src, "write_schema")
}

sparkDropTable <- function(con, schema, name) {
  sql <- glue::glue("DROP TABLE IF EXISTS {quoteName(fullName(schema, name))}")
  DBI::dbExecute(conn = con, statement = sql)
  invisible()
}
sparkComputeTable <- function(x, con, schema, name, temporary) {
  temporary <- checkTemporary(temporary = temporary, schema = schema)
  if (temporary) {
    schema <- schema[names(schema) == "prefix"]
  }

  fullname <- fullName(schema = schema, name = name)

  # drop table if already exists
  sparkDropTable(con = con, schema = schema, name = name)

  # compute table
  if (temporary) {
    create <- "CREATE TEMPORARY TABLE"
  } else {
    create <- "CREATE TABLE"
  }
  q <- dbplyr::sql_render(query = x, con = con)
  sql <- "{create} {quoteName(fullname)} AS {q}" |>
    glue::glue()
  DBI::dbExecute(conn = con, statement = sql)

  # reference table
  dplyr::tbl(con, fullname)

  fullNameQuoted <- DBI::dbQuoteIdentifier(conn = con, x = fullname)

  sql <- glue::glue("SELECT * INTO {fullNameQuoted}
                      FROM ({dbplyr::sql_render(x)}) x")

  DBI::dbSendQuery(con, sql)

}
checkTemporary <- function(temporary, schema) {
  if (isTRUE(temporary)) return(TRUE)
  !any(c("catalog", "schema") %in% names(schema))
}
