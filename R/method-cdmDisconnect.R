
#' Disconnect the connection of the cdm object
#'
#' @param cdm cdm reference
#' @param dropWriteSchema Whether to drop tables in the writeSchema
#' @param dropTempSchema Whether to drop tables in the tempSchema
#' @param ... Not used
#'
#' @export
cdmDisconnect.spark_cdm <- function(cdm, dropWriteSchema = FALSE, dropTempSchema = FALSE, ...) {
  # input check
  omopgenerics::assertLogical(dropWriteSchema, length = 1)
  omopgenerics::assertLogical(dropTempSchema, length = 1)

  on.exit(sparklyr::spark_disconnect(attr(attr(cdm, "cdm_source"), "con")), add = TRUE)

  # drop tables if needed
  if (dropWriteSchema) {
    schema <- writeSchema(src = cdm)
    nms <- sparkListTables(con = con, schema = schema)
    purrr::map(nms, \(x) sparkDropTable(con = con, schema = schema, name = x))
  }

  # drop temp tables if needed
  if (dropTempSchema) {
    schema <- tempSchema(src = cdm)
    nms <- sparkListTables(con = con, schema = schema)
    purrr::map(nms, \(x) sparkDropTable(con = con, schema = schema, name = x))
  }

  return(invisible(TRUE))
}
