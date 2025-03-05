#' @export
dropSourceTable.spark_cdm <- function(cdm, name) {
  sparkDropTable(con = getCon(cdm), schema = writeSchema(cdm), name = name)
}

sparkDropTable <- function(con, schema, name) {
  DBI::dbExecute(con, glue::glue("DROP TABLE IF EXISTS {fullName(schema, name)}"))
  invisible(TRUE)
}

sparkDropDataFrame <- function(con, name){
  con %>%
    sparklyr::spark_session() |>
    sparklyr::invoke("catalog") |>
    sparklyr::invoke("dropTempView", name)
}
