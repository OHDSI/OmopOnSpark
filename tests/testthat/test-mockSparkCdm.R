test_that("basic mock cdm", {
  # skip_on_cran()
  # if(sparklyr::spark_installed_versions() |> nrow() == 0){
  #   skip()
  # }
  # folder <- file.path(tempdir(), "temp_spark")
  # options(sparklyr.verbose = TRUE)
  # cdm <- mockSparkCdm(path = folder)
  # expect_true(is.null(cdmSchema(cdm)))
  # expect_true(is.null(writeSchema(cdm)))
  # expect_true(writePrefix(cdm) == "my_study_")
  # # overwrite
  # cdm <- mockSparkCdm(path = folder)
  # person_5 <- cdm$person |>
  #   head(5) |>
  #   dplyr::collect()
  # expect_true(nrow(person_5) == 5)
  #
  # # analytics works
  # expect_no_error(snapshot <- OmopSketch::summariseOmopSnapshot(cdm))
  #
  # cdmDisconnect(cdm)
  # unlink(folder, recursive = TRUE)
})
