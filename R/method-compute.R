#' @export
compute.spark_cdm <- function(x, name, temporary = FALSE, overwrite = TRUE, ...) {
  # check source and name
  src <- attr(x, "tbl_source")
  currentName <- attr(x, "tbl_name")

  # get attributes
  schema <- schemaToWrite(src, temporary)
  con <- getCon(src)

  if (identical(currentName, name)) {
    intermediate <- omopgenerics::uniqueTableName()
    sparkComputeTable(query = x, schema = schema, name = intermediate)
    x <- sparkReadTable(con = con, schema = schema, name = intermediate)
    on.exit(sparkDropTable(con = con, schema = schema, name = intermediate))
  }

  sparkComputeTable(query = x, schema = schema, name = name)
  x <- sparkReadTable(con = con, schema = schema, name = name) |>
    omopgenerics::newCdmTable(src = src, name = name)
  return(x)
}

sparkComputeTable <- function(query, schema, name) {
  sparklyr::spark_write_table(
    x = query, name = fullName(schema, name), mode = "overwrite"
  )
}
