sparkSource <- function(con, cdmSchema, writeSchema, writePrefix = NULL) {
  con <- validateConnection(con)
  # writeSchema <- validateSchema(writeSchema, FALSE)
  # create source
  newSparkSource(
    con = con,
    cdmSchema = cdmSchema,
    writeSchema = writeSchema,
    writePrefix = writePrefix
  )
}

newSparkSource <- function(con, cdmSchema, writeSchema, writePrefix) {
  tempPrefix <- paste0("temp_", paste0(sample(letters, 5), collapse = ""), "_")
  sourceName <- dplyr::case_when(
    con_type(con) == "odbc" ~ "ODBC",
    con_type(con)  == "jdbc" ~ "DatabaseConnector",
    con_type(con) == "sparlyr" ~ "Sparklyr",
    .default = "spark"
  )

  structure(
    .Data = list(),
    con = con,
    cdm_schema = cdmSchema,
    write_schema = writeSchema,
    write_prefix = writePrefix,
    class = "spark_cdm"
  ) |>
    omopgenerics::newCdmSource(sourceType = sourceName)
}

