#' @export
readSourceTable.spark_cdm <- function(cdm, name) {
  sparkReadTable(con = getCon(cdm), schema = writeSchema(cdm), name = name)
}

sparkReadTable <- function(con, schema, name) {
  dplyr::tbl(con, fullName(schema, name))
}
