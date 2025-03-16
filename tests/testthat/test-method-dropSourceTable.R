test_that("dropping tables", {

  folder <- file.path(tempdir(), "drop_spark")
  cdm <- mockSparkCdm(path = folder)

  cdm <- insertTable(
    cdm = cdm,
    name = "cars",
    table = cars,
    temporary = FALSE
  )
  expect_true("cars" %in% sparkListTables(
    con = getCon(cdm),
    schema = writeSchema(cdm),
    prefix = writePrefix(cdm)
  ))
  expect_true("cohort" %in% sparkListTables(
    con = getCon(cdm),
    schema = writeSchema(cdm),
    prefix = writePrefix(cdm)
  ))
  omopgenerics::dropSourceTable(cdm = cdm, name = "cars")
  expect_false("cars" %in% sparkListTables(
    con = getCon(cdm),
    schema = writeSchema(cdm),
    prefix = writePrefix(cdm)
  ))
  expect_true("cohort" %in% sparkListTables(
    con = getCon(cdm),
    schema = writeSchema(cdm),
    prefix = writePrefix(cdm)
  ))
  # using tidyselect
  cdm <- insertTable(
    cdm = cdm,
    name = "cars",
    table = cars,
    temporary = FALSE
  )
  expect_true("cars" %in% sparkListTables(
    con = getCon(cdm),
    schema = writeSchema(cdm),
    prefix = writePrefix(cdm)
  ))
  expect_true("cohort" %in% sparkListTables(
    con = getCon(cdm),
    schema = writeSchema(cdm),
    prefix = writePrefix(cdm)
  ))
  omopgenerics::dropSourceTable(cdm = cdm, name = dplyr::starts_with("ca"))
  expect_false("cars" %in% sparkListTables(
    con = getCon(cdm),
    schema = writeSchema(cdm),
    prefix = writePrefix(cdm)
  ))
  expect_true("cohort" %in% sparkListTables(
    con = getCon(cdm),
    schema = writeSchema(cdm),
    prefix = writePrefix(cdm)
  ))


  # behavour if no table exists - no error, nothing dropped
  omopgenerics::dropSourceTable(cdm = cdm,
                                name = "not_a_table")
  expect_true("cohort" %in% sparkListTables(
    con = getCon(cdm),
    schema = writeSchema(cdm),
    prefix = writePrefix(cdm)
  ))

  # behaviour if temp table - no error, nothing dropped
  cdm$new <-  cdm$person |>
    dplyr::compute()
  omopgenerics::dropSourceTable(cdm = cdm,
                                name = "new")
  expect_no_error(new <- cdm$new |>
    dplyr::collect())

  # behaviour if table is in write schema - no error, nothing dropped
  omopgenerics::dropSourceTable(cdm = cdm,
                                name = "person")
  expect_no_error(person <- cdm$person |>
                    dplyr::collect())

  cdmDisconnect(cdm)
  unlink(folder, recursive = TRUE)

})
