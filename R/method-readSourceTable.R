#' @export
readSourceTable.spark_cdm <- function(cdm, name) {
  sparkReadTable(con = getCon(cdm), schema = writeSchema(cdm), name = name)
}

sparkReadTable <- function(con, schema, prefix = NULL, name) {
  if(is.null(prefix)){
  tbl_name <- paste0(schema, ".", name)
  } else {
  tbl_name <- paste0(schema, ".", prefix, name)
  }
  dplyr::tbl(con, I(tbl_name))
}
