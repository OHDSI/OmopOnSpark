
# returns either "odbc" or "sparklyr"
# will give error if odbc is set up using native query
con_type <- function(con){
  if(inherits(con, "spark_connection")){
    "sparklyr"
  } else if("odbc.version" %in% names(DBI::dbGetInfo(con) |> unlist())){
    "odbc"
  } else {
    cli::cli_abort("Unkown connection type")
  }
}

validateConnection <- function(con, call = parent.frame()) {
  if(con_type(con) == "odbc"){
    validateOdbcConnection(con)
  } else {
    validateSparklyrConnection(con)
  }
  return(con)
}

validateOdbcConnection <- function(con){
  if(isTRUE(native_odbc_con(con))){
    cli::cli_abort("Native query via ODBC is not currently supported.")
  }
}
# checks if odbc connection has been set to use natice queries (TRUE) or not (FALSE)
native_odbc_con <- function(con){
  tryCatch({
    tblName <- omopgenerics::uniqueTableName()
    DBI::dbWriteTable(conn = con,
                      name = tblName,
                      value = cars |> head(1))
    DBI::dbRemoveTable(conn = con,
                       name = tblName)
    FALSE
  }, error = function(e) {
    TRUE
  })
}

validateSparklyeConnection <- function(con){
if (!sparklyr::connection_is_open(con)) {
  cli::cli_abort(c(x = "{.arg con} connection is closed, please provide an open connection."), call = call)
}
}
