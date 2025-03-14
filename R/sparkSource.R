
#' Create a spark source object.
#'
#' @param con A connection to a spark database.
#' @param cdmSchema A schema for the OMOP CDM core tables.
#' @param writeSchema A write schema with writing permissions.
#' @param writePrefix A write prefix used whenever writing tables in the write
#' schema.
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
sparkSource <- function(con, cdmSchema, writeSchema, writePrefix = NULL) {
  con <- validateConnection(con)
  # writeSchema <- validateSchema(writeSchema, FALSE)
  # create source
  newSparkSource(
    con = con,
    cdmSchema = cdmSchema,
    writeSchema = writeSchema,
    writePrefix = writePrefix
  )
}

newSparkSource <- function(con, cdmSchema, writeSchema, writePrefix) {
  tempPrefix <- paste0("temp_", paste0(sample(letters, 5), collapse = ""), "_")
  structure(
    .Data = list(),
    con = con,
    cdm_schema = cdmSchema,
    write_schema = writeSchema,
    write_prefix = writePrefix,
    class = "spark_cdm"
  ) |>
    omopgenerics::newCdmSource(sourceType = "sparklyr")
}

# internal functions
cdmSchema <- function(src) {
  if(inherits(src, "cdm_reference")){
    return(attr(omopgenerics::cdmSource(src), "cdm_schema"))
  } else {
    attr(src, "cdm_schema")
  }
}
writeSchema <- function(src) {
  if(inherits(src, "cdm_reference")){
    return(attr(omopgenerics::cdmSource(src), "write_schema"))
  } else {
    attr(src, "write_schema")
  }
}
writePrefix <- function(src) {
  if(inherits(src, "cdm_reference")){
    return(attr(omopgenerics::cdmSource(src), "write_prefix"))
  } else {
    attr(src, "write_prefix")
  }
}
getCon <- function(src) {
  if(inherits(src, "cdm_reference")){
    return(attr(omopgenerics::cdmSource(src), "con"))
  } else {
    attr(src, "con")
  }
}
schemaToWrite <- function(src, temporary) {
  writeSchema(src)
}
