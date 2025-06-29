#' @export
readSourceTable.spark_cdm <- function(cdm, name) {
  sparkReadTable(con = getCon(cdm), schema = writeSchema(cdm), name = name)
}

sparkReadTable <- function(con, schema, prefix = NULL, name) {
  if(!is.null(schema)){
  if (is.null(prefix)) {
    tbl_name <- paste0(schema, ".", name)
  } else {
    tbl_name <- paste0(schema, ".", prefix, name)
  }}

  if(is.null(schema)){
    if (is.null(prefix)) {
      tbl_name <- paste0(name)
    } else {
      tbl_name <- paste0(prefix, name)
    }}
  dplyr::tbl(con, I(tbl_name))
}
