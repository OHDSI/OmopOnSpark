
#' Drop temporary tables.
#'
#' @param src A `cdm_reference` object created with `cdmFromSpark()`, a
#' `spark_cdm` source object or a `spark_connection` object.
#' @param schema A schema, lists are used for schemas, example: schema =
#' `list(catalog = "my_catalog", schema = "my_schema")`
#'
#' @return Invisible TRUE.
#' @export
#'
dropTempTables <- function(src, schema) {
  # schema <- validateSchema(schema, FALSE)
  if (inherits(src, "cdm_reference")) {
    src <- omopgenerics::cdmSource(src)
  }
  if (inherits(src, "spark_cdm")) {
    src <- getCon(src)
  } else if (!inherits(src, "spark_connection")) {
    c(x = "{.var src} has to be: a `cdm_reference` object created with `cdmFromSpark()`, a {.cls spark_cdm} source or a {.cls spark_connection} object") |>
      cli::cli_abort()
  }

  if (!sparklyr::connection_is_open(src)) {
    cli::cli_abort(c(x = "Connection is closed."))
  }

  schema$prefix <- "temp_"
  nms <- sparkListTables(con = src, schema = schema)
  cli::cli_inform(c(i = "{length(nms)} temporary table{?s} indentified."))
  purrr::map(nms, \(x) sparkDropTable(con = src, schema = schema, name = x))

  invisible(TRUE)
}
