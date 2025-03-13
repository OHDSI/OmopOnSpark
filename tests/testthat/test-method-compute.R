test_that("compute working", {

folder <- file.path(tempdir(), "temp_spark_compute")
cdm <- mockSparkCdm(folder)

cdm$person_2 <- cdm$person |>
  dplyr::compute()

expect_true(inherits(cdm$person_2, "cdm_table"))
expect_true(inherits(cdm$person_2, "tbl_spark"))
expect_true(inherits(cdm$person_2, "tbl_sql"))

expect_identical(cdm$person |>
  dplyr::count() |>
  dplyr::collect(),
cdm$person |>
  dplyr::count() |>
  dplyr::compute() |>
  dplyr::collect())

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
})
