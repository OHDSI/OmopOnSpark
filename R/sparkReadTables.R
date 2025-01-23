#' This function reads a table from a specified schema and table name in a Spark connection.
#'
#' @param con A valid Spark connection object. This is typically created using `sparklyr::spark_connect()`.
#' @param schema A list specifying the schema and the catalog where the table is located and any prefix
#' @param tableName A string specifying the name of the table to read from the Spark connection.
#'
#' @return A `tbl` object from the `dplyr` package, representing the Spark table specified by the `schema` and `tableName`.

sparkReadTable <- function(con, schema, tableName){
  full_name <- paste(schema$catalog, schema$schema, paste0(schema$prefix, tableName), sep = ".")
  return(dplyr::tbl(con, I(full_name)))
}


