
#' Create a spark source object.
#'
#' @param con A connection to a spark database.
#' @param writeSchema A write schema with writing permissions.
#' @param tempSchema A write schema with writing permissions to emulate
#' temporary tables.
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
sparkSource <- function(con, writeSchema, tempSchema = writeSchema) {
  con <- validateConnection(con)
  writeSchema <- validateSchema(writeSchema, FALSE)
  tempSchema <- validateSchema(tempSchema, FALSE)
  # create source
  newSparkSource(
    con = con,
    writeSchema = writeSchema,
    tempSchema = tempSchema
  )
}

newSparkSource <- function(con, writeSchema, tempSchema) {
  tempPrefix <- paste0("temp_", paste0(sample(letters, 5), collapse = ""), "_")
  tempSchema$prefix <- tempPrefix
  structure(
    .Data = list(),
    con = con,
    write_schema = writeSchema,
    temp_schema = tempSchema,
    class = "spark_cdm"
  ) |>
    omopgenerics::newCdmSource(sourceType = "sparklyr")
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
