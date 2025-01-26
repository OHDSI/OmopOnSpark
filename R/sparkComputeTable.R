#'
#' This function computes a table from a Spark query and saves it with the specified table name, either as a temporary table or a permanent table
#'
#' @param query A Spark query or a spark dataframe object representing the data to compute and save.
#' @param tableName A string specifying the name of the table to save.
#' @param schema A list specifying the schema details for the table. The list may include:
#'   - `catalog`:  The catalog name.
#'   - `schema`:  The schema name.
#'   - `prefix`: (Optional) A prefix to the table name.
#'
#' @param temporary A logical value indicating whether to create a temporary table (`TRUE`) or a permanent table (`FALSE`). Defaults to `FALSE`.
#'
#' @return
#' - If `temporary = TRUE`, the function returns a registered temporary table in Spark
#' - If `temporary = FALSE`, the function writes the table to the specified schema
#'
sparkComputeTable <- function(query, tableName, schema = list(), temporary = FALSE) {
  if (temporary) {

    return(sparklyr::sdf_register(query, name = paste0(schema$prefix, tableName)))

  } else {

    full_name <- paste(schema$catalog, schema$schema, paste0(schema$prefix, tableName), sep = ".")

    return(sparklyr::spark_write_table(query, name = full_name, mode = "overwrite"))

  }
}
