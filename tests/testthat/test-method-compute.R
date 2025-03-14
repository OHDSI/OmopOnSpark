test_that("compute from permanent to temp", {
  folder <- file.path(tempdir(), "temp_spark_compute")
  cdm <- mockSparkCdm(folder)

  cdm$person_2 <- cdm$person |>
    dplyr::compute()

  expect_true(inherits(cdm$person_2, "cdm_table"))
  expect_true(inherits(cdm$person_2, "tbl_spark"))
  expect_true(inherits(cdm$person_2, "tbl_sql"))
  expect_true(is.na(omopgenerics::tableName(cdm$person_2)))

  expect_identical(
    cdm$person |>
      dplyr::count() |>
      dplyr::collect(),
    cdm$person |>
      dplyr::count() |>
      dplyr::compute() |>
      dplyr::collect()
  )

  # original table unaffected
  start_count <- cdm$person |>
    dplyr::tally() |>
    dplyr::pull("n")

  cdm$person_3 <- cdm$person |>
    head(1) |>
    dplyr::compute()

  end_count <- cdm$person |>
    dplyr::tally() |>
    dplyr::pull("n")

  expect_identical(start_count, end_count)

  # overwrite table
  expect_no_error(cdm$person_2 <- cdm$person_2 |>
    head(1) |>
    dplyr::compute(name = "person_2"))

  cdmDisconnect(cdm)
  unlink(folder, recursive = TRUE)
})

test_that("compute from permanent to permanent", {
  folder <- file.path(tempdir(), "temp_spark_compute")
  cdm <- mockSparkCdm(folder)

  cdm$person_2 <- cdm$person |>
    dplyr::compute(
      temporary = FALSE,
      name = "person_2"
    )

  expect_true(inherits(cdm$person_2, "cdm_table"))
  expect_true(inherits(cdm$person_2, "tbl_spark"))
  expect_true(inherits(cdm$person_2, "tbl_sql"))
  expect_true(omopgenerics::tableName(cdm$person_2) == "person_2")

  expect_identical(
    cdm$person |>
      dplyr::count() |>
      dplyr::collect(),
    cdm$person |>
      dplyr::count() |>
      dplyr::compute() |>
      dplyr::collect()
  )

  # original table unaffected
  start_count <- cdm$person |>
    dplyr::tally() |>
    dplyr::pull("n")

  cdm$person_3 <- cdm$person |>
    head(1) |>
    dplyr::compute(
      temporary = TRUE,
      name = "person_3"
    )

  end_count <- cdm$person |>
    dplyr::tally() |>
    dplyr::pull("n")

  expect_identical(start_count, end_count)

  # overwrite table
  expect_no_error(cdm$person_2 <- cdm$person_2 |>
    head(1) |>
    dplyr::compute(
      temporary = TRUE,
      name = "person_2"
    ))

  cdmDisconnect(cdm)
  unlink(folder, recursive = TRUE)
})

test_that("compute from temp to permanent", {
  folder <- file.path(tempdir(), "temp_spark_compute")
  cdm <- mockSparkCdm(folder)

  cdm$person_2 <- cdm$person |>
    dplyr::compute()

  cdm$person_3 <- cdm$person |>
    dplyr::compute(
      name = "person_3",
      temporary = FALSE
    )

  expect_true(inherits(cdm$person_2, "cdm_table"))
  expect_true(inherits(cdm$person_2, "tbl_spark"))
  expect_true(inherits(cdm$person_2, "tbl_sql"))
  expect_true(is.na(omopgenerics::tableName(cdm$person_2)))
  expect_true(omopgenerics::tableName(cdm$person_3) == "person_3")

  # error if we try to overwrite an existing table
  expect_error(cdm$person |>
    dplyr::compute(
      name = "person_3",
      temporary = FALSE,
      overwrite = FALSE
    ))

  cdmDisconnect(cdm)
  unlink(folder, recursive = TRUE)
})
