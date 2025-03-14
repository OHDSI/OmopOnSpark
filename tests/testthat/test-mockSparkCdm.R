test_that("basic mock cdm", {

  folder <- file.path(tempdir(), "temp_spark")
  cdm <- mockSparkCdm(path = folder)
  expect_true(cdmSchema(cdm) == "omop")
  expect_true(writeSchema(cdm) == "results")
  expect_true(writePrefix(cdm) == "my_study_")
  # overwrite
  cdm <- mockSparkCdm(path = folder)
  person_5 <- cdm$person |>
    head(5) |>
    dplyr::collect()
  expect_true(nrow(person_5) == 5)
  cdmDisconnect(cdm)
  unlink(folder, recursive = TRUE)

})
