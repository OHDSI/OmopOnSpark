test_that("multiplication works", {
  expect_equal(2 * 2, 4)
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

  disconnect(con)

})
