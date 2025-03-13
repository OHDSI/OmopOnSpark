#' @export
insertTable.spark_cdm <- function(cdm,
                                  name,
                                  table,
                                  overwrite = TRUE,
                                  temporary = FALSE) {

  if(inherits(cdm, "cdm_source")){
  insertTableToSparkSource(cdm = cdm,
                           name = name,
                           table = table,
                           overwrite = overwrite,
                           temporary = temporary)
  } else if (inherits(cdm, "cdm_reference")) {
    insertTableToSparkCdmReference(cdm = cdm,
                             name = name,
                             table = table,
                             overwrite = overwrite,
                             temporary = temporary)
  } else {
    cli::cli_abort("cdm must be a cdm reference or a cdm source")
  }


}

insertTableToSparkSource <- function(cdm,
                                     name,
                                     table,
                                     overwrite,
                                     temporary){
  # get attributes
  schema <- schemaToWrite(cdm, temporary)
  con <- getCon(cdm)

  # check overwrite
  if (overwrite) {
    sparkDropTable(con = con, schema = schema, name = name)
  } else if (name %in% sparkListTables(con = con, schema = schema)) {
    cli::cli_abort(c(
      x = "Table {.pkg {name}} already exists use `overwrite = FALSE`."
    ))
  }

  # write table
  sparkWriteTable(con = con, schema = schema, name = name, value = table)

  # read table
  sparkReadTable(con = con, schema = schema, name = name) |>
    omopgenerics::newCdmTable(src = cdm, name = name)

}

insertTableToSparkCdmReference <- function(cdm,
                                           name,
                                           table,
                                           overwrite,
                                           temporary){
  # get attributes
  schema <- schemaToWrite(attr(cdm, "cdm_source"), temporary)
  con <- getCon(attr(cdm, "cdm_source"))

  # check overwrite
  if (overwrite) {
    sparkDropTable(con = con, schema = schema, name = name)
  } else if (name %in% sparkListTables(con = con, schema = schema)) {
    cli::cli_abort(c(
      x = "Table {.pkg {name}} already exists use `overwrite = FALSE`."
    ))
  }

  # write table
  if(isTRUE(temporary)){
    tmp_tbl <- sparklyr::sdf_copy_to(con,
                                     table,
                                     name = omopgenerics::uniqueTableName(),
                                     overwrite = TRUE)
    cdm[[name]] <-  tmp_tbl |>
      omopgenerics::newCdmTable(src = attr(cdm, "cdm_source"), name = name)
  } else {
    sparkWriteTable(con = con, schema = schema, name = name, value = table)
    cdm[[name]] <- sparkReadTable(con = con, schema = schema, name = name) |>
      omopgenerics::newCdmTable(src = attr(cdm, "cdm_source"), name = name)
  }

  cdm

}

sparkWriteTable <- function(con, schema, name, value) {

  # take into account catalog, schema and prefix
  fullname <- fullName(schema, name)
  # first insert data as a spark dataframe
  tmp_tbl <- omopgenerics::uniqueTableName()
  spark_df <- sparklyr::sdf_copy_to(con,
                                    value,
                                    name = tmp_tbl,
                                    overwrite = TRUE)
  # now as a spark table
  sparklyr::spark_write_table(x = spark_df,
                              name = fullname,
                              mode = "overwrite")
  # drop spark dataframe and rm to remove from rstudio pane
  sparkDropDataFrame(con = con, name = tmp_tbl)
  rm(spark_df)

  return(invisible(NULL))
}

