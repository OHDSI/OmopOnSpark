cdmSchema <- function(src) {
  if (inherits(src, "cdm_reference")) {
    return(attr(omopgenerics::cdmSource(src), "cdm_schema"))
  } else {
    attr(src, "cdm_schema")
  }
}
cdmPrefix <- function(src) {
  if (inherits(src, "cdm_reference")) {
    return(attr(omopgenerics::cdmSource(src), "cdm_prefix"))
  } else {
    attr(src, "cdm_prefix")
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

# utilities to support tests
getTestCon <- function(test_db, folder = NULL){

  if(test_db == "sparklyr"){
      working_config <- sparklyr::spark_config()
      working_config$spark.sql.warehouse.dir <- folder
      con <- tryCatch(sparklyr::spark_connect(
        master = "local",
        config = working_config),
        error = function(e){
          cli::cli_warn("Could not connect via odbc")
          return(NULL)})
      return(con)
  }

  if(test_db == "odbc"){
    con <- tryCatch( DBI::dbConnect(
        odbc::databricks(),
        httpPath = Sys.getenv("DATABRICKS_HTTPPATH"),
        useNativeQuery = FALSE),
      error = function(e){
        cli::cli_warn("Could not connect via odbc")
        return(NULL)})
    return(con)
  }
  if(test_db == "jdbc"){
    connectionDetails <- tryCatch(
      DatabaseConnector::createConnectionDetails(
        dbms = "spark",
        user = Sys.getenv('DATABRICKS_USER'),
        password = Sys.getenv('DATABRICKS_TOKEN'),
        connectionString = paste0(Sys.getenv('DATABRICKS_CONNECTION_STRING'), "EnableArrow=0;")
      ),
      error = function(e){
        return(NULL)})
    con <-  tryCatch(
      DatabaseConnector::connect(connectionDetails),
      error = function(e){
        cli::cli_warn("Could not connect via jdbc")
        return(NULL)})
    return(con)
  }

  cli::cli_abort("{test_db} not supported")
}
createEmptyTestSchema <- function(con, test_schema){
  if(!is.null(test_schema)){
    cli::cli_inform("Creating schema {test_schema}")
    executeQuery(con, glue::glue("CREATE SCHEMA IF NOT EXISTS {test_schema}"))
  }
}
removeTestSchema <- function(con, test_schema){
  if(!is.null(test_schema)){
    cli::cli_inform("Removing schema {test_schema}")
    executeQuery(con, glue::glue("DROP SCHEMA IF EXISTS {test_schema} CASCADE"))
  }
}
