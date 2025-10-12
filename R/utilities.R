cdmSchema <- function(src) {
  if (inherits(src, "cdm_reference")) {
    return(attr(omopgenerics::cdmSource(src), "cdm_schema"))
  } else {
    attr(src, "cdm_schema")
  }
}
writeSchema <- function(src) {
  if (inherits(src, "cdm_reference")) {
    return(attr(omopgenerics::cdmSource(src), "write_schema"))
  } else {
    attr(src, "write_schema")
  }
}
writePrefix <- function(src) {
  if (inherits(src, "cdm_reference")) {
    return(attr(omopgenerics::cdmSource(src), "write_prefix"))
  } else {
    attr(src, "write_prefix")
  }
}
getCon <- function(src) {
  if (inherits(src, "cdm_reference")) {
    return(attr(omopgenerics::cdmSource(src), "con"))
  } else {
    attr(src, "con")
  }
}
getWriteTableName <- function(writeSchema, prefix, name) {
  if(!is.null(writeSchema)){
  if (is.null(prefix)) {
    tbl_name <- paste0(writeSchema, ".", name)
  } else {
    tbl_name <- paste0(writeSchema, ".", prefix, name)
  }
  }

  if(is.null(writeSchema)){
    if (is.null(prefix)) {
      tbl_name <- name
    } else {
      tbl_name <- paste0(prefix, name)
    }
  }
  tbl_name
}
getExistingTables <- function(con, schema){

if(is.null(schema)){
dbTbls <-  DBI::dbGetQuery(con, glue::glue("SHOW TABLES")) |>
  dplyr::as_tibble()
} else {
dbTbls <-  DBI::dbGetQuery(con, glue::glue("SHOW TABLES IN {schema}")) |>
  dplyr::as_tibble()
}
colnames(dbTbls) <- tolower(colnames(dbTbls))
dbTbls |>
  dplyr::pull("tablename")
}
executeQuery <- function(con, query){
  if (con_type(con) == "jdbc") {
    DatabaseConnector::dbExecute(con, query)
  } else {
    DBI::dbExecute(con, query)
  }
}
getFullName <- function(schema, prefix, name){
  if(stringr::str_count(schema, pattern = "\\.") > 0){
    # includes catalog, so will split out
    workingCatalog <- gsub("\\..*", "", schema)
    workingSchema <- gsub(".*\\.", "", schema)
    workingName <- paste0(prefix, name)
    fullName <- DBI::Id(workingCatalog, workingSchema, workingName)
  } else {
    fullName <- DBI::Id(schema, paste0(prefix, name))
  }
  fullName
}
