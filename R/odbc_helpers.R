native_odbc_con <- function(con) {
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


