test_that("basic mock cdm", {

  folder <- file.path(tempdir(), "temp_spark")
  cdm <- mockSparkCdm(path = folder)
  # overwrite
  cdm <- mockSparkCdm(path = folder)
  person_5 <- cdm$person |>
    head(5) |>
    dplyr::collect()
  expect_true(nrow(person_5) == 5)
  cdmDisconnect(cdm)

})
