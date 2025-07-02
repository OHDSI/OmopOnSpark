test_that("creating a cdm reference - sparklyr", {
  skip_on_cran()
  if(sparklyr::spark_installed_versions() |> nrow() == 0){
    skip()
  }

  cdm_local <- omock::mockCdmReference() |>
    omock::mockPerson(nPerson = 100) |>
    omock::mockObservationPeriod() |>
    omock::mockConditionOccurrence() |>
    omock::mockCohort(name = "my_cohort")

  folder <- file.path(tempdir(), "temp_spark")
  working_config <- sparklyr::spark_config()
  working_config$spark.sql.warehouse.dir <- folder

  sc <- sparklyr::spark_connect(
    master = "local",
    config = working_config
  )

  src <- sparkSource(
    con = sc,
    cdmSchema = NULL,
    writeSchema = NULL,
    writePrefix = "study_1_"
  )

  insertCdmTo(cdm_local, src)

  cdm <- cdmFromSpark(
    con = sc,
    cdmSchema = NULL,
    writeSchema = NULL,
    cohortTables = "my_cohort",
    cdmName = "my spark cdm",
    .softValidation = TRUE,
    writePrefix = "study_1_"
  )
  expect_identical(writeSchema(cdm), NULL)
  expect_identical(omopgenerics::cdmName(cdm), "my spark cdm")
  expect_identical(omopgenerics::cdmVersion(cdm), "5.3")
  expect_true(inherits(cdm, "cdm_reference"))
  expect_true(inherits(cdm$person, "cdm_table"))
  expect_true(inherits(cdm$person, "omop_table"))

  expect_identical(
    sort(names(cdm_local)),
    sort(names(cdm))
  )

  expect_identical(
    omopgenerics::attrition(cdm_local$my_cohort),
    omopgenerics::attrition(cdm$my_cohort)
  )

  expect_identical(
    omopgenerics::settings(cdm_local$my_cohort),
    omopgenerics::settings(cdm$my_cohort)
  )

  cdmDisconnect(cdm)
  unlink(folder, recursive = TRUE)
})

test_that("cdm validation - sparklyr", {
  skip_on_cran()
  if(sparklyr::spark_installed_versions() |> nrow() == 0){
    skip()
  }
 cdm <- mockSparkCdm(file.path(tempdir(), "temp_spark"))
  expect_no_error(omopgenerics::validateCdmArgument(cdm,
    checkOverlapObservation = TRUE,
    validation = "error"
  ))
  expect_no_error(omopgenerics::validateCdmArgument(cdm,
    checkStartBeforeEndObservation = TRUE,
    validation = "error"
  ))
  expect_no_error(omopgenerics::validateCdmArgument(cdm,
    checkPlausibleObservationDates = TRUE,
    validation = "error"
  ))

  cdmDisconnect(cdm)
  unlink(folder, recursive = TRUE)
})
