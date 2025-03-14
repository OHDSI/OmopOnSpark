#' @export
insertTable.spark_cdm <- function(cdm,
                                  name,
                                  table,
                                  overwrite = TRUE,
                                  temporary = FALSE) {
  if (inherits(cdm, "cdm_source")) {
    insertTableToSparkSource(
      cdm = cdm,
      name = name,
      table = table,
      overwrite = overwrite,
      temporary = temporary
    )
  } else if (inherits(cdm, "cdm_reference")) {
    insertTableToSparkCdmReference(
      cdm = cdm,
      name = name,
      table = table,
      overwrite = overwrite,
      temporary = temporary
    )
  } else {
    cli::cli_abort("cdm must be a cdm reference or a cdm source")
  }
}

insertTableToSparkSource <- function(cdm,
                                     name,
                                     table,
                                     overwrite,
                                     temporary) {
  # get attributes
  schema <- writeSchema(cdm)
  prefix <- writePrefix(cdm)
  con <- getCon(cdm)

  # check overwrite
  if (overwrite) {
    sparkDropTable(con = con, schema = schema, prefix = prefix, name = name)
  } else if (name %in% sparkListTables(con = con, schema = schema, prefix = prefix)) {
    cli::cli_abort(c(
      x = "Table {.pkg {name}} already exists use `overwrite = FALSE`."
    ))
  }


  if (isTRUE(temporary)) {
    sparklyr::sdf_copy_to(con,
      table,
      name = omopgenerics::uniqueTableName(),
      overwrite = TRUE
    ) |>
      omopgenerics::newCdmTable(
        src = cdm,
        name = NA_character_
      )
  } else {
    sparkInsertTable(con = con, schema = schema, prefix = prefix, name = name, value = table)
    sparkReadTable(con = con, schema = schema, prefix = prefix, name = name) |>
      omopgenerics::newCdmTable(src = cdm, name = name)
  }
}

insertTableToSparkCdmReference <- function(cdm,
                                           name,
                                           table,
                                           overwrite,
                                           temporary) {
  # get attributes
  schema <- writeSchema(cdm)
  prefix <- writePrefix(cdm)
  con <- getCon(cdm)

  # check overwrite
  if (overwrite) {
    sparkDropTable(con = con, schema = schema, prefix = prefix, name = name)
  } else if (name %in% sparkListTables(con = con, schema = schema, prefix = prefix)) {
    cli::cli_abort(c(
      x = "Table {.pkg {name}} already exists use `overwrite = FALSE`."
    ))
  }

  # write table
  if (isTRUE(temporary)) {
    tmp_tbl <- sparklyr::sdf_copy_to(con,
      table,
      name = omopgenerics::uniqueTableName(),
      overwrite = TRUE
    )
    cdm[[name]] <- tmp_tbl |>
      omopgenerics::newCdmTable(
        src = attr(cdm, "cdm_source"),
        name = NA_character_
      )
  } else {
    sparkInsertTable(con = con, schema = schema, prefix = prefix, name = name, value = table)
    cdm[[name]] <- sparkReadTable(con = con, schema = schema, prefix = prefix, name = name) |>
      omopgenerics::newCdmTable(src = attr(cdm, "cdm_source"), name = name)
  }

  cdm
}

# insert a local dataframe as a spark table (a permanent table)
sparkInsertTable <- function(con, schema, prefix, name, value) {

  tbl_name <- getWriteTableName(writeSchema = schema,
                                prefix = prefix,
                                name = name)
  # first insert data as a spark dataframe
  tmp_tbl <- omopgenerics::uniqueTableName()
  spark_df <- sparklyr::sdf_copy_to(con,
    value,
    name = tmp_tbl,
    overwrite = TRUE
  )
  # now as a spark table
  sparklyr::spark_write_table(
    x = spark_df,
    name = tbl_name,
    mode = "overwrite"
  )
  # drop spark dataframe and rm to remove from rstudio pane
  sparkDropDataFrame(con = con, name = tmp_tbl)
  rm(spark_df)

  return(invisible(NULL))
}
