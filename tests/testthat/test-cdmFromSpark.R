test_that("cdmFromSpark", {

  skip_on_cran()

  for(i in seq_along(dbToTest)){

    workingDb <- dbToTest[i]
    cli::cli_alert("Testing cdmFromSpark() against {workingDb}")

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


      # simple cdm, adding example data for person and observation period
      createOmopTablesOnSpark(con, schemaName = test_schema, cdmVersion = "5.3")

      cdm_spark <- cdmFromSpark(
        con = con,
        cdmName = "test",
        cdmSchema = test_schema,
        writeSchema = test_schema,
        .softValidation = TRUE
      )

      cdm_local <- omock::mockCdmReference() |>
        omock::mockPerson(nPerson = 100) |>
        omock::mockObservationPeriod()

      cdm_spark <- insertTable(cdm = cdm_spark,
                               name = "person",
                               table = cdm_local$person)
      cdm_spark <- insertTable(cdm = cdm_spark,
                               name = "observation_period",
                               table = cdm_local$observation_period)
      expect_no_error(cdm_spark <- cdmFromSpark(
        con = con,
        cdmName = "test",
        cdmSchema = test_schema,
        writeSchema = test_schema,
        .softValidation = TRUE
      ))


      expect_identical(
        cdm_spark$person |>
        dplyr::pull("person_id") |>
        sort(),
        cdm_local$person |>
          dplyr::pull("person_id") |>
          sort())
      expect_identical(
        cdm_spark$person |>
          dplyr::pull("gender_concept_id") |>
          sort(),
        cdm_local$person |>
          dplyr::pull("gender_concept_id") |>
          sort())
      expect_identical(
        cdm_spark$person |>
          dplyr::pull("race_concept_id") |>
          sort(),
        cdm_local$person |>
          dplyr::pull("race_concept_id") |>
          sort())
      # problem on sparklyr with datetime when all are missing
      # expect_identical(
      #   cdm_spark$person |>
      #     dplyr::pull("birth_datetime") |>
      #     sort(),
      #   cdm_local$person |>
      #     dplyr::pull("birth_datetime") |>
      #     sort())


      # clean up
      dropSourceTable(cdm_spark, dplyr::everything())
      removeTestSchema(con, test_schema)


      # same, but with prefixed omop tables
      createOmopTablesOnSpark(con, schemaName = test_schema, cdmVersion = "5.3",
                              cdmPrefix = "p1_")
      cdm_spark <- cdmFromSpark(
        con = con,
        cdmName = "test",
        cdmSchema = test_schema,
        writeSchema = test_schema,
        cdmPrefix = "p1_",
        writePrefix = "p2_",
        .softValidation = TRUE
      )
      cdm_spark <- insertTable(cdm = cdm_spark,
                               name = "person",
                               table = cdm_local$person)
      cdm_spark <- insertTable(cdm = cdm_spark,
                               name = "observation_period",
                               table = cdm_local$observation_period)

      expect_identical(
        cdm_spark$person |>
          dplyr::pull("person_id") |>
          sort(),
        cdm_local$person |>
          dplyr::pull("person_id") |>
          sort())
      expect_identical(
        cdm_spark$person |>
          dplyr::pull("gender_concept_id") |>
          sort(),
        cdm_local$person |>
          dplyr::pull("gender_concept_id") |>
          sort())
      expect_identical(
        cdm_spark$person |>
          dplyr::pull("race_concept_id") |>
          sort(),
        cdm_local$person |>
          dplyr::pull("race_concept_id") |>
          sort())
      # problem on sparklyr with datetime when all are missing
      # expect_identical(
      #   cdm_spark$person |>
      #     dplyr::pull("birth_datetime") |>
      #     sort(),
      #   cdm_local$person |>
      #     dplyr::pull("birth_datetime") |>
      #     sort())


      expect_true(omopgenerics::cdmVersion(cdm_spark) == "5.3")
      expect_identical(omopgenerics::omopColumns(table = "visit_occurrence", version = "5.3"),
                       colnames(cdm_spark$visit_occurrence))

      # cdm validation
      expect_no_error(omopgenerics::validateCdmArgument(cdm_spark,
                                                        checkOverlapObservation = TRUE,
                                                        validation = "error"
      ))
      expect_no_error(omopgenerics::validateCdmArgument(cdm_spark,
                                                        checkStartBeforeEndObservation = TRUE,
                                                        validation = "error"
      ))
      expect_no_error(omopgenerics::validateCdmArgument(cdm_spark,
                                                        checkPlausibleObservationDates = TRUE,
                                                        validation = "error"
      ))

      # clean up
      dropSourceTable(cdm_spark, dplyr::everything())
      removeTestSchema(con, test_schema)

      cdmDisconnect(cdm_spark)
    }
  }

})

