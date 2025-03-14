test_that("dropping tables", {

  folder <- file.path(tempdir(), "drop_spark")
  cdm <- mockSparkCdm(path = folder)

  cdm <- insertTable(cdm = cdm,
              name = "cars",
              table = cars,
              temporary = FALSE)
  expect_true("cars" %in% sparkListTables(con = getCon(cdm),
                                  schema = writeSchema(cdm),
                                  prefix = writePrefix(cdm)))
  expect_true("cohort" %in% sparkListTables(con = getCon(cdm),
                                          schema = writeSchema(cdm),
                                          prefix = writePrefix(cdm)))
  omopgenerics::dropSourceTable(cdm = cdm, name = "cars")
  expect_true(!"cars" %in% sparkListTables(con = getCon(cdm),
                                          schema = writeSchema(cdm),
                                          prefix = writePrefix(cdm)))
  expect_true("cohort" %in% sparkListTables(con = getCon(cdm),
                                            schema = writeSchema(cdm),
                                            prefix = writePrefix(cdm)))
  # using tidyselect
  cdm <- insertTable(cdm = cdm,
                     name = "cars",
                     table = cars,
                     temporary = FALSE)
  expect_true("cars" %in% sparkListTables(con = getCon(cdm),
                                          schema = writeSchema(cdm),
                                          prefix = writePrefix(cdm)))
  expect_true("cohort" %in% sparkListTables(con = getCon(cdm),
                                            schema = writeSchema(cdm),
                                            prefix = writePrefix(cdm)))
  omopgenerics::dropSourceTable(cdm = cdm, name = dplyr::starts_with("ca"))
  expect_true(!"cars" %in% sparkListTables(con = getCon(cdm),
                                           schema = writeSchema(cdm),
                                           prefix = writePrefix(cdm)))
  expect_true("cohort" %in% sparkListTables(con = getCon(cdm),
                                            schema = writeSchema(cdm),
                                            prefix = writePrefix(cdm)))

  cdmDisconnect(cdm)
  unlink(folder, recursive = TRUE)

})
