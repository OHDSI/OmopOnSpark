#' Disconnect the connection of the cdm object
#'
#' @param cdm cdm reference
#' @param dropWriteSchema Whether to drop tables in the writeSchema
#' @param ... Not used
#'
#' @export
cdmDisconnect.spark_cdm <- function(cdm, dropWriteSchema = FALSE, ...) {
  # input check
  omopgenerics::assertLogical(dropWriteSchema, length = 1)
  con <- attr(attr(cdm, "cdm_source"), "con")
  on.exit(sparklyr::spark_disconnect(con), add = TRUE)

  # drop tables if needed
  if (dropWriteSchema) {
    schema <- writeSchema(src = cdm)
    nms <- sparkListTables(con = con, schema = schema)
    purrr::map(nms, \(x) sparkDropTable(con = con, schema = schema, name = x))
  }

  return(invisible(TRUE))
}
