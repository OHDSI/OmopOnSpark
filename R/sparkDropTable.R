#' Drop a Table from Spark
#'
#' This function drops a specified table from a schema in a Spark connection, if it exists.
#'
#' @param con A valid Spark connection object. This is typically created using `sparklyr::spark_connect()`.
#' @param schema A list specifying the schema details where the table is located. The list must contain:
#'   - `schema`: A string specifying the schema name.
#'   - `catalog`: A string specifying the catalog name.
#' @param tableName A string specifying the name of the table to drop.
#'
#' @return The result of the `DROP TABLE` query execution as returned by `DBI::dbGetQuery()`.
#'
#' @examples
#' \dontrun{
#' # Establish a Spark connection
#' con <- sparklyr::spark_connect(master = ...)
#'
#' # Drop a table from the default schema
#' sparkDropTable(con, schema = list("schema" = "default"), tableName = "my_table")
#'
#' # Disconnect from Spark
#' sparklyr::spark_disconnect(con)
#' }
#'
sparkDropTable <- function(con, schema, tableName) {
  full_name <- paste(schema$catalog, schema$schema, paste0(schema$prefix, tableName), sep = ".")
  return(DBI::dbGetQuery(con, paste("DROP TABLE IF EXISTS", full_name)))

}

