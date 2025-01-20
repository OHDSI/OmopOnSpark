
#' Create a spark source object.
#'
#' @param con A connection to a spark database.
#' @param writeSchema A write schema with writing permissions. Use NULL to use
#' temp tables.
#'
#' @return A spark_cdm object.
#' @export
#'
#' @examples
#' \dontrun{
#' con <- sparklyr::spark_connect(master = "local")
#' sparkSource(con)
#' }
#'
sparkSource <- function(con, writeSchema = NULL) {
  # validate connection
  con <- validateConnection(con)

  # validate writeSchema
  writeSchema <- validateSchema(writeSchema)
  # check writing permissions

  # create source
  structure(
    .Data = list(), con = con, write_schema = writeSchema, class = "spark_cdm"
  ) |>
    omopgenerics::newCdmSource()
}
