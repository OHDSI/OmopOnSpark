test_that("creating a cdm reference", {

  cdm_local <- omock::mockCdmReference() |>
    omock::mockPerson(nPerson = 100) |>
    omock::mockObservationPeriod() |>
    omock::mockConditionOccurrence() |>
    omock::mockCohort(name = "my_cohort")

  folder <- file.path(tempdir(), "temp_spark")
  working_config <- sparklyr::spark_config()
  working_config$spark.sql.warehouse.dir <- folder
  con <- sparklyr::spark_connect(master = "local", config = working_config)
  createSchema(con = con, schema = list(schema = "my_schema", prefix = "test_"))
  src <- sparkSource(con = con, writeSchema = list(schema = "my_schema", prefix = "test_"))

  insertCdmTo(cdm_local, src)

  # without validation
  cdm <- cdmFromSpark(con = con,
               cdmSchema = list(schema = "my_schema", prefix = "test_"),
               writeSchema = list(schema = "my_schema", prefix = "test_"),
               cohortTables = "my_cohort",
               cdmName = "my spark cdm",
               .softValidation = TRUE)

  # with validation
  # cdm <- cdmFromSpark(con = con,
  #                     cdmSchema = list(schema = "my_schema", prefix = "test_"),
  #                     writeSchema = list(schema = "my_schema", prefix = "test_"),
  #                     cohortTables = "my_cohort",
  #                     cdmName = "my spark cdm")

  expect_identical(omopgenerics::cdmName(cdm), "my spark cdm")
  expect_identical(omopgenerics::cdmVersion(cdm), "5.3")
  expect_true(inherits(cdm, "cdm_reference"))
  expect_true(inherits(cdm$person, "cdm_table"))
  expect_true(inherits(cdm$person, "omop_table"))

  expect_identical(
    sort(names(cdm_local)),
    sort(names(cdm)))

  expect_identical(
  omopgenerics::attrition(cdm_local$my_cohort),
  omopgenerics::attrition(cdm$my_cohort))

  expect_identical(
    omopgenerics::settings(cdm_local$my_cohort),
    omopgenerics::settings(cdm$my_cohort))

  cdmDisconnect(con)

})

test_that("cdm validation", {

  cdm_local <- omock::mockCdmReference() |>
    omock::mockPerson(nPerson = 100) |>
    omock::mockObservationPeriod()

  folder <- file.path(tempdir(), "temp_spark")
  working_config <- sparklyr::spark_config()
  working_config$spark.sql.warehouse.dir <- folder
  con <- sparklyr::spark_connect(master = "local", config = working_config)
  createSchema(con = con, schema = list(schema = "my_schema", prefix = "test_"))
  src <- sparkSource(con = con, writeSchema = list(schema = "my_schema", prefix = "test_"))

  insertCdmTo(cdm_local, src)

  cdm <- cdmFromSpark(con = con,
                      cdmSchema = list(schema = "my_schema", prefix = "test_"),
                      writeSchema = list(schema = "my_schema", prefix = "test_"),
                      .softValidation = TRUE)

  expect_no_error(omopgenerics::validateCdmArgument(cdm_local,
                                    checkOverlapObservation = TRUE,
                                    validation = "error"))
  # expect_no_error(omopgenerics::validateCdmArgument(cdm,
  #                                   checkOverlapObservation = TRUE,
  #                                   validation = "error"))

  expect_no_error(omopgenerics::validateCdmArgument(cdm_local,
                                                    checkStartBeforeEndObservation = TRUE,
                                                    validation = "error"))
  # expect_no_error(omopgenerics::validateCdmArgument(cdm,
  #                                   checkStartBeforeEndObservation = TRUE,
  #                                   validation = "error"))

  expect_no_error(omopgenerics::validateCdmArgument(cdm_local,
                                                    checkPlausibleObservationDates = TRUE,
                                                    validation = "error"))
  expect_no_error(omopgenerics::validateCdmArgument(cdm,
                                                    checkPlausibleObservationDates = TRUE,
                                                    validation = "error"))


  expect_identical(
  sort(cdm_local$observation_period |>
    dplyr::pull("observation_period_start_date")),
  sort(cdm$observation_period |>
         dplyr::pull("observation_period_start_date"))
  )

  cdmDisconnect(con)

})
