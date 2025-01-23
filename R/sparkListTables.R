#'
#' This function lists the tables available in a specified schema within a Spark connection.
#'
#' @param con A valid Spark connection object. This is typically created using `sparklyr::spark_connect()`.
#' @param schema A list specifying the schema and the catalog where the tables are located
#'
#' @return A character vector of table names available in the specified catalog and schema within the Spark connection.
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
#' tables_in_schema <- sparkListTables(con, schema = list(catalog = "cat", schema = "my_schema"))
#'
#' # Disconnect from Spark
#' sparklyr::spark_disconnect(con)
#' }

sparkListTables <- function(con, schema) {

  full_name <-  paste(schema$catalog, schema$schema, sep = ".")

  return(DBI::dbGetQuery(con, sprintf("SHOW TABLES IN %s", full_name)))

}

