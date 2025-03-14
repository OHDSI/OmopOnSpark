#' @export
compute.spark_cdm <- function(x, name, temporary = FALSE, overwrite = TRUE, ...) {
  # check source and name
  src <- attr(x, "tbl_source")
  currentName <- attr(x, "tbl_name")

  # get attributes
  schema <- writeSchema(src)
  prefix <- writePrefix(src)
  con <- getCon(src)

  if (isFALSE(overwrite)) {
 if (name %in% sparkListTables(con = con, schema = schema, prefix = prefix)) {
    cli::cli_abort(c(
      x = "Table {.pkg {name}} already exists use `overwrite = TRUE` to overwrite."
    ))
 }}

  if (identical(currentName, name)) {
    intermediate <- omopgenerics::uniqueTableName()
    sparkComputeTable(query = x, schema = schema, prefix = prefix, name = intermediate)
    x <- sparkReadTable(con = con, schema = schema, prefix = prefix, name = intermediate)
    on.exit(sparkDropTable(con = con,
                           schema = schema,
                           prefix = prefix,
                           name = intermediate))
  }

  if(isTRUE(temporary)){
    sparkComputeTemporaryTable(con = con, query = x) |>
      omopgenerics::newCdmTable(src = src, name = NA_character_)
  } else {
  sparkComputeTable(query = x, schema = schema, prefix = prefix, name = name)
  sparkReadTable(con = con, schema = schema, prefix = prefix, name = name) |>
    omopgenerics::newCdmTable(src = src, name = name)
  }

}

sparkComputeTable <- function(query, schema, prefix, name) {
  if(is.null(prefix)){
    tbl_name <- paste0(schema, ".", name)
  } else {
    tbl_name <- paste0(schema, ".", prefix, name)
  }
  sparklyr::spark_write_table(
    x = query, name = tbl_name, mode = "overwrite"
  )
}

sparkComputeTemporaryTable <- function(con, query){
  sparklyr::sdf_copy_to(con,
                        query,
                        name = omopgenerics::uniqueTableName(),
                        overwrite = TRUE)
}
