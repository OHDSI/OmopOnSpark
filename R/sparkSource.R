
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
    omopgenerics::newCdmSource()
}
