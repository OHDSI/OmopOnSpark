#' @export
listSourceTables.spark_cdm <- function(cdm) {
  sparkListTables(con = getCon(cdm), schema = writeSchema(cdm), prefix = writePrefix(cdm))
}

sparkListTables <- function(con, schema, prefix) {
  if(!is.null(schema)){
    x <- getExistingTables(con = con, schema = schema)
  } else {
    x <- getExistingTables(con = con, schema = NULL)
  }

  if (!is.null(prefix)) {
    x <- x[startsWith(x = x, prefix = prefix)]
    x <- substr(x, nchar(prefix) + 1, nchar(x))
  }
  x <- x[nchar(x) > 0]
  return(x)
}
