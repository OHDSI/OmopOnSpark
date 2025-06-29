test_that("disconnect connection", {

  folder <- file.path(tempdir(), omopgenerics::uniqueTableName())
  cdm <- mockSparkCdm(folder)

  expect_true(sparklyr::connection_is_open(getCon(cdm)))
  cdmDisconnect(cdm)
  expect_false(sparklyr::connection_is_open(getCon(cdm)))

  unlink(folder, recursive = TRUE)
})

test_that("dropping emulated temp tables", {

  folder <- file.path(tempdir(), omopgenerics::uniqueTableName())
  cdm <- mockSparkCdm(path = folder)

  start_tbl <- list.files(folder,recursive = TRUE)

  # compute two temp tables
  # will be in the write schema starting with "tmp_og_"
  cdm$my_temp_table <- cdm$person |>
    head(1) |>
    dplyr::compute()
  cdm$another <- cdm$person |>
    head(1) |>
    dplyr::compute()

  # we will have multiple temp tables emulated
  # expect_true(length(setdiff(list.files(folder,recursive = TRUE),
  #                            start_tbl)) > 0)

  cdmDisconnect(cdm)

  # expect_true(length(setdiff(list.files(folder,recursive = TRUE),
  #                            start_tbl)) == 0)

  unlink(folder, recursive = TRUE)

})
