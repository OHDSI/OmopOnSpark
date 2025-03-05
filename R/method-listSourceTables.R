#' @export
listSourceTables.spark_cdm <- function(cdm) {
  sparkListTables(con = getCon(cdm), schema = writeSchema(cdm))
}

sparkListTables <- function(con, schema) {
  schemaName <- paste0(c(schema$catalog, schema$schema), collapse = ".")
  x <- DBI::dbGetQuery(con, glue::glue("SHOW TABLES IN {schemaName}")) |>
    dplyr::as_tibble() |>
    dplyr::filter(!.data$isTemporary) |>
    dplyr::pull("tableName")
  if (length(schema$prefix) > 0) {
    x <- x[startsWith(x = x, prefix = schema$prefix)]
    x <- substr(x, nchar(schema$prefix) + 1, nchar(x))
  }
  x <- x[nchar(x) > 0]
  return(x)
}
