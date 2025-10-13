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
  prefix <- cdmPrefix(cdm)
  schema <- writeSchema(cdm)
  con <- attr(attr(cdm, "cdm_source"), "con")
  if(con_type(con) == "sparklyr"){
  on.exit(sparklyr::spark_disconnect(con), add = TRUE)
  }

  # drop emulated temp tables
  nms_tmp <- sparkListTables(con = con, schema = schema, prefix = "tmp_og_")
  purrr::map(nms_tmp, \(x) sparkDropTable(con = con, schema = schema, name = x, prefix = "tmp_og_"))

  # drop tables if needed
  if (dropWriteSchema) {
    schema <- writeSchema(src = cdm)
    nms <- sparkListTables(con = con, schema = schema, prefix = prefix)
    purrr::map(nms, \(x) sparkDropTable(con = con, schema = schema, name = x, prefix = prefix))
  }

  return(invisible(TRUE))
}
