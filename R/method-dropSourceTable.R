#' @export
dropSourceTable.spark_cdm <- function(cdm, name) {
  sparkDropTable(con = getCon(cdm),
                 schema = writeSchema(cdm),
                 prefix = writePrefix(cdm),
                 name = name)
}

sparkDropTable <- function(con, schema, prefix, name) {
  if(is.null(prefix)){
    tbl_name <- paste0(schema, ".", name)
  } else {
    tbl_name <- paste0(schema, ".", prefix, name)
  }
  DBI::dbExecute(con, glue::glue("DROP TABLE IF EXISTS {tbl_name}"))
  invisible(TRUE)
}

sparkDropDataFrame <- function(con, name){
  con |>
    sparklyr::spark_session() |>
    sparklyr::invoke("catalog") |>
    sparklyr::invoke("dropTempView", name)
}
