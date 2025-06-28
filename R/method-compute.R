#' @export
compute.spark_cdm <- function(x, name, temporary = FALSE, overwrite = TRUE, ...) {
  src <- attr(x, "tbl_source")
  schema <- writeSchema(src)
  prefix <- writePrefix(src)
  con <- getCon(src)
  if (isFALSE(overwrite)) {
    if (name %in% sparkListTables(con = con, schema = schema, prefix = prefix)) {
      cli::cli_abort(c(
        x = "Table {.pkg {name}} already exists use `overwrite = TRUE` to overwrite."
      ))
    }
  }

  if(con_type(con) == "sparklyr"){
  computeSparklyr(x = x, name = name, temporary = temporary, overwrite = overwrite)
  } else {
  computeSparkOdbc(x = x, name = name, temporary = temporary, overwrite = overwrite)
  }

}


computeSparkOdbc <- function(x, name, temporary, overwrite){

  # check source and name
  src <- attr(x, "tbl_source")
  # get attributes
  currentName <- attr(x, "tbl_name")
  schema <- writeSchema(src)
  prefix <- writePrefix(src)
  con <- getCon(src)

  if (identical(currentName, name)) {
    intermediate_tbl <- omopgenerics::uniqueTableName(prefix = prefix)
    intermediate_query_sql <- dbplyr::build_sql("CREATE TABLE ",
                                   dbplyr::ident(schema),
                                   dbplyr::sql("."),
                                   dbplyr::ident(intermediate_tbl),
                                   dbplyr::sql(" USING DELTA"),
                                   " AS ", dbplyr::sql_render(x),
                                   con = con)
    DBI::dbExecute(con, intermediate_query_sql)
    x <- dplyr::tbl(con, Id(schema, intermediate_tbl))
  }

  if (isTRUE(overwrite)) {
    if (name %in% sparkListTables(con = con, schema = schema, prefix = prefix)) {
      DBI::dbRemoveTable(con, name = DBI::Id(schema, paste0(prefix, name)))
    }
  }

  query_sql <- dbplyr::build_sql("CREATE TABLE ",
                                 dbplyr::ident(schema),
                                 dbplyr::sql("."),
                                 dbplyr::ident(paste0(prefix, name)),
                                 dbplyr::sql(" USING DELTA"),
                           " AS ", dbplyr::sql_render(x),
                           con = con)
  DBI::dbExecute(con, query_sql)
  # cache_sql <- dbplyr::build_sql("CACHE TABLE ",
  #                                dbplyr::ident(schema),
  #                                dbplyr::sql("."),
  #                                dbplyr::ident(paste0(prefix, name)),
  #                                con = con)
  # DBI::dbExecute(con, cache_sql)
  dplyr::tbl(con, Id(schema, paste0(prefix, name)))

  # query_sql <- dbplyr::build_sql("CREATE OR REPLACE TEMP VIEW ",
  #                                dbplyr::ident(paste0(prefix, name)),
  #                                " AS ", dbplyr::sql_render(x),
  #                                con = con)
  #
  # DBI::dbExecute(con, query_sql)
  # dplyr::tbl(con, Id(paste0(prefix, name)))

}
computeSparklyr <- function(x, name, temporary, overwrite){
  # check source and name
  src <- attr(x, "tbl_source")
  # get attributes
  currentName <- attr(x, "tbl_name")
  schema <- writeSchema(src)
  prefix <- writePrefix(src)
  con <- getCon(src)


  if (identical(currentName, name)) {
    intermediate <- omopgenerics::uniqueTableName()
    sparklyrComputeTable(query = x, schema = schema, prefix = prefix, name = intermediate)
    x <- sparkReadTable(con = con, schema = schema, prefix = prefix, name = intermediate)
    on.exit(sparkDropTable(
      con = con,
      schema = schema,
      prefix = prefix,
      name = intermediate
    ))
  }

  if (isTRUE(temporary)) {
    temp_name <- omopgenerics::uniqueTableName(prefix = "tmp_")
    sparklyrComputeTable(query = x, schema = schema, prefix = prefix, name = temp_name)
    sparkReadTable(con = con, schema = schema, prefix = prefix, name = temp_name) |>
      omopgenerics::newCdmTable(src = src, name = NA_character_)
  } else {
    sparklyrComputeTable(query = x, schema = schema, prefix = prefix, name = name)
    sparkReadTable(con = con, schema = schema, prefix = prefix, name = name) |>
      omopgenerics::newCdmTable(src = src, name = name)
  }
}
sparklyrComputeTable <- function(query, schema, prefix, name) {
  if (is.null(prefix)) {
    tbl_name <- paste0(schema, ".", name)
  } else {
    tbl_name <- paste0(schema, ".", prefix, name)
  }
  sparklyr::spark_write_table(
    x = query, name = tbl_name, mode = "overwrite"
  )
}

# Below not working on databricks
# sparkComputeTemporaryTable <- function(con, query) {
#   sparklyr::sdf_copy_to(con,
#     query,
#     name = omopgenerics::uniqueTableName(),
#     overwrite = TRUE
#   )
# }
