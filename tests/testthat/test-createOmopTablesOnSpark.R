test_that("create omop cdm tables", {

  skip_on_cran()

  for(i in seq_along(dbToTest)){

  workingDb <- dbToTest[i]
  cli::cli_alert("Testing createOmopTablesOnSpark() against {workingDb}")

  if(workingDb == "sparklyr") {
    folder <- file.path(tempdir(), "temp_spark")
    con <-  getTestCon("sparklyr", folder)
    test_schema <- NULL
  } else if(workingDb == "odbc") {
   con <-  getTestCon("odbc")
   test_schema <- omopgenerics::uniqueTableName()
  } else if (workingDb == "jdbc") {
    con <-  getTestCon("jdbc")
    test_schema <- omopgenerics::uniqueTableName()
  } else{
    cli::cli_abort("{workingDb} not supported")
  }

  if(is.null(con)){
    cli::cli_inform(" - Skipping tests for {workingDb}")
  } else {
  createEmptyTestSchema(con, test_schema)

  createOmopTablesOnSpark(con, schemaName = test_schema, cdmVersion = "5.3", cdmPrefix = "test_")

 cdm <- cdmFromSpark(
   con = con,
   cdmSchema = test_schema,
   writeSchema = test_schema,
   cdmPrefix = "test_",
   cdmName = "my spark cdm",
   writePrefix = "test_",
   .softValidation = TRUE
 )
 expect_true(omopgenerics::cdmVersion(cdm) == "5.3")
 expect_no_error(omopgenerics::validateCdmArgument(cdm))
 expect_identical(omopgenerics::omopColumns(table = "visit_occurrence", version = "5.3"),
                  colnames(cdm$visit_occurrence))

  # won't overwrite existing tables
 expect_error(createOmopTablesOnSpark(con, schemaName = test_schema, cdmVersion = "5.4", cdmPrefix = "test_"))

 # will work if we use a different cdm prefix
 createOmopTablesOnSpark(con, schemaName = test_schema, cdmVersion = "5.4", cdmPrefix = "test_2_")
 cdm_2 <- cdmFromSpark(
   con = con,
   cdmSchema = test_schema,
   writeSchema = test_schema,
   cdmPrefix = "test_2_",
   cdmName = "my spark cdm",
   writePrefix = "test_2_tbl_",
   cdmVersion = "5.4",
   .softValidation = TRUE
 )

 expect_true(omopgenerics::cdmVersion(cdm_2) == "5.4")
 expect_no_error(omopgenerics::validateCdmArgument(cdm_2))
 expect_identical(omopgenerics::omopColumns(table = "visit_occurrence", version = "5.4"),
                  colnames(cdm_2$visit_occurrence))


  # clean up
 dropSourceTable(cdm, dplyr::everything())
 dropSourceTable(cdm_2, dplyr::everything())
 removeTestSchema(con, test_schema)

 cdmDisconnect(cdm)
}
  }

})

