#' @export
dropSourceTable.spark_cdm <- function(cdm, name) {
  namesCdm <- names(cdm)
  namesSource <- listSourceTables(cdm = cdm)
  toDrop <- c(namesCdm, namesSource) |>
    unique() |>
    selectTables(name = name)
  toDropCdm <- namesCdm[namesCdm %in% toDrop]
  toDropSource <- namesSource[namesSource %in% toDrop]

  for (i in seq_along(toDropSource)) {
    sparkDropTable(
      con = getCon(cdm),
      schema = writeSchema(cdm),
      prefix = writePrefix(cdm),
      name = toDropSource[i]
    )
  }
}

selectTables <- function(tables, name) {
  res <- tables |>
    rlang::set_names() |>
    as.list() |>
    dplyr::as_tibble() |>
    dplyr::select(dplyr::any_of(name)) |>
    colnames()
  # drop settings, attrition and/or codelist tables
  res <- res |>
    purrr::map(\(x) paste0(x, c("", "_set", "_attrition", "_codelist"))) |>
    purrr::flatten_chr()
  unique(res[res %in% tables])
}

sparkDropTable <- function(con, schema, prefix, name) {
  tbl_name <- getWriteTableName(writeSchema = schema, prefix = prefix, name = name)
  DBI::dbExecute(con, glue::glue("DROP TABLE IF EXISTS {tbl_name}"))
  invisible(TRUE)
}

sparkDropDataFrame <- function(con, name) {
  con |>
    sparklyr::spark_session() |>
    sparklyr::invoke("catalog") |>
    sparklyr::invoke("dropTempView", name)
}
