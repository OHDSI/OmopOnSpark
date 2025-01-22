#' This function reads a table from a specified schema and table name in a Spark connection.
#'
#' @param con A valid Spark connection object. This is typically created using `sparklyr::spark_connect()`.
#' @param schema A list specifying the schema (and the catalog if present) where the table is located
#' @param tableName A string specifying the name of the table to read from the Spark connection.
#'
#' @return A `tbl` object from the `dplyr` package, representing the Spark table specified by the `schema` and `tableName`.
#'
#' @details
#' - The function validates the input Spark connection using `validateConnection()`.
#' - It constructs the full name of the table using the `fullName()` function with the provided `schema` and `tableName`.
#' - The function then reads the table from the Spark connection using `dplyr::tbl()`, which returns a reference to the table.
#'
#' @examples
#' \dontrun{
#' # Establish a Spark connection
#' con <- sparklyr::spark_connect(master = ...)
#'
#' # Read a table from the default schema
#' table_data <- sparkReadTable(con, schema = list(schema = "default"), tableName = "my_table")
#'
#' # Disconnect from Spark
#' sparklyr::spark_disconnect(con)
#' }
#'
#' @seealso [sparklyr::spark_connect()], [dplyr::tbl()]
#'
#' @export
sparkReadTable <- function(con, schema, tableName){

  con <- validateConnection(con)
  schema <- validateSchema(schema)

  if ("catalog" %in% names(schema)){

    full_name <-  fullName(schema = schema, name = tableName)

    return(dplyr::tbl(con, full_name))
  }

  return(dplyr::tbl(con, tableName))
}


