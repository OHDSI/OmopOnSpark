#'
#' This function lists the tables available in a specified schema within a Spark connection.
#'
#' @param con A valid Spark connection object. This is typically created using `sparklyr::spark_connect()`.
#' @param schema A list containing the schema to connect to. The default is `list("schema" = "default")`. If more schemas are available, the function connects to the one specified in `schema$schema`.
#'
#' @return A character vector of table names available in the specified schema within the Spark connection.
#'
#' @examples
#' \dontrun{
#' # Establish a Spark connection
#' con <- sparklyr::spark_connect(...)
#'
#' # List tables in the default schema
#' tables <- sparkListTables(con)
#'
#' # List tables in a specific schema
#' tables_in_schema <- sparkListTables(con, schema = list("schema" = "my_schema"))
#'
#' # Disconnect from Spark
#' sparklyr::spark_disconnect(con)
#' }
#'
#' @export
sparkListTables <- function(con, schema = list("schema" = "default")) {
  con <- validateConnection(con)
  schema <- validateSchema(schema)
  sparklyr::tbl_change_db(con, schema$schema) # if there are more schemas, we connect to the one in schema$schema
  return(DBI::dbListTables(con))
}

