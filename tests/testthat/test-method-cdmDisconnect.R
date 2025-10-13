test_that("cdmDisconnect", {

  skip_on_cran()

  for(i in seq_along(dbToTest)){

    workingDb <- dbToTest[i]
    cli::cli_alert("Testing cdmDisconnect() against {workingDb}")

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

      createOmopTablesOnSpark(con, schemaName = test_schema, cdmVersion = "5.3")

      cdm_local <- omock::mockCdmReference() |>
        omock::mockPerson(nPerson = 100) |>
        omock::mockObservationPeriod() |>
        omock::mockCohort()

      cdm_spark <- cdmFromSpark(
        con = con,
        cdmName = "test",
        cdmSchema = test_schema,
        writeSchema = test_schema,
        .softValidation = TRUE
      )

      cdm_spark <- insertTable(cdm = cdm_spark,
                               name = "person",
                               table = cdm_local$person)
      cdm_spark <- insertTable(cdm = cdm_spark,
                               name = "observation_period",
                               table = cdm_local$observation_period)
      cdm_spark <- insertTable(cdm = cdm_spark,
                               name = "cohort",
                               table = cdm_local$cohort)
      cdm_spark$cohort <- omopgenerics::newCohortTable(cdm_spark$cohort)

      # compute two temp tables
      # will be in the write schema starting with "tmp_og_"
      cdm_spark$my_temp_table <- cdm_spark$person |>
        head(1) |>
        dplyr::compute()
      cdm_spark$another <- cdm_spark$person |>
        head(1) |>
        dplyr::compute()

      cdmDisconnect(cdm_spark, dropWriteSchema = TRUE)

      if(workingDb == "sparklyr") {
        expect_false(sparklyr::connection_is_open(getCon(cdm_spark)))
      }

      # reconnect
      if(workingDb == "sparklyr") {
        con <-  getTestCon("sparklyr", folder)
      } else if(workingDb == "odbc") {
        con <-  getTestCon("odbc")
      } else if (workingDb == "jdbc") {
        con <-  getTestCon("jdbc")
      }

      src <- sparkSource(con, cdmSchema = test_schema, writeSchema = test_schema)
      # we shoulld have just a single table, created when running sparkSource() above
      expect_true(length(listSourceTables(src)) == 1)
    }
  }

})
