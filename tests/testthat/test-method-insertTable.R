test_that("insert permanent table to cdm ref", {
  skip_on_cran()

  path <- file.path(tempdir(), "temp_spark")
  cdm <- mockSparkCdm(path)

  # insert a permanent table
  cdm <- insertTable(
    cdm = cdm,
    name = "cars",
    table = cars,
    overwrite = TRUE,
    temporary = FALSE
  )
  # we should get back our cdm reference with the table added
  expect_true("cars" %in% names(cdm))
  expect_identical(omopgenerics::tableName(cdm$cars), "cars")
  expect_no_error(omopgenerics::validateCdmTable(cdm$cars))

  expect_identical(
    cdm$cars |>
      dplyr::collect() |>
      dplyr::arrange("speed", "dist"),
    dplyr::tibble(cars) |>
      dplyr::arrange("speed", "dist")
  )

  # will be in write schema and use write prefix
  expect_identical(
    dplyr::tbl(
      getCon(cdm),
      I(paste0(writeSchema(cdm), ".", writePrefix(cdm), "cars"))
    ) |>
      dplyr::collect() |>
      dplyr::arrange("speed", "dist"),
    dplyr::tibble(cars) |>
      dplyr::arrange("speed", "dist")
  )
  # will not be able to reference the table without the prefix
  expect_error(dplyr::tbl(
    getCon(cdm),
    I(paste0(writeSchema(cdm), ".", "cars"))
  ))

  # insert and overwrite
  expect_no_error(insertTable(
    cdm = cdm,
    name = "cars",
    table = cars,
    overwrite = TRUE,
    temporary = FALSE
  ))

  # overwrite false with existing table - expected error
  expect_error(cdm <- insertTable(
    cdm = cdm,
    name = "cars",
    table = cars,
    overwrite = FALSE,
    temporary = FALSE
  ))

  expect_no_error(cdm <- insertTable(
    cdm = cdm,
    name = "cars_2",
    table = cars,
    overwrite = FALSE,
    temporary = FALSE
  ))
  expect_true("cars_2" %in% names(cdm))

  cdmDisconnect(cdm)
  unlink(folder, recursive = TRUE)
})

test_that("insert temp table to cdm ref", {
  skip_on_cran()

  path <- file.path(tempdir(), "temp_spark")
  cdm <- mockSparkCdm(path)

  # insert a temp table
  cdm <- insertTable(
    cdm = cdm,
    name = "cars",
    table = cars,
    overwrite = TRUE,
    temporary = TRUE
  )
  # we should get back our cdm reference with the table added
  expect_true("cars" %in% names(cdm))
  expect_true(is.na(omopgenerics::tableName(cdm$cars)))
  expect_no_error(omopgenerics::validateCdmTable(cdm$cars))

  expect_identical(
    cdm$cars |>
      dplyr::collect() |>
      dplyr::arrange("speed", "dist"),
    dplyr::tibble(cars) |>
      dplyr::arrange("speed", "dist")
  )

  # insert and overwrite
  expect_no_error(insertTable(
    cdm = cdm,
    name = "cars",
    table = cars,
    overwrite = TRUE,
    temporary = TRUE
  ))

  # overwrite false with existing table
  # this will no longer cause an error when using temp tables
  expect_no_error(cdm <- insertTable(
    cdm = cdm,
    name = "cars",
    table = cars,
    overwrite = FALSE,
    temporary = TRUE
  ))

  expect_no_error(cdm <- insertTable(
    cdm = cdm,
    name = "cars_2",
    table = cars,
    overwrite = FALSE,
    temporary = TRUE
  ))
  expect_true("cars_2" %in% names(cdm))

  cdmDisconnect(cdm)
  unlink(folder, recursive = TRUE)
})

test_that("test insert to cdm source - no prefix ", {
  skip_on_cran()

  con <- sparklyr::spark_connect(master = "local", config = config)
  DBI::dbExecute(con, glue::glue("CREATE SCHEMA IF NOT EXISTS my_schema"))
  src <- sparkSource(
    con = con,
    cdmSchema = "omop",
    writeSchema = "my_schema",
    writePrefix = NULL
  )

  tab1 <- insertTable(src, name = "my_table", table = cars, overwrite = TRUE, temporary = FALSE)
  expect_true(inherits(tab1, "cdm_table"))
  expect_identical(dbplyr::remote_name(tab1), "my_table")
  expect_identical(omopgenerics::tableName(tab1), "my_table")

  tab2 <- insertTable(src, name = "my_table", table = cars, overwrite = TRUE, temporary = TRUE)
  expect_true(inherits(tab2, "cdm_table"))
  expect_true(is.na(omopgenerics::tableName(tab2)))

  sparklyr::spark_disconnect(con)
  unlink(folder, recursive = TRUE)
})

test_that("test insert to cdm source - prefix ", {
  skip_on_cran()

  con <- sparklyr::spark_connect(master = "local", config = config)
  DBI::dbExecute(con, glue::glue("CREATE SCHEMA IF NOT EXISTS my_schema"))
  src <- sparkSource(
    con = con, cdmSchema = "omop",
    writeSchema = "my_schema", writePrefix = "prefix_"
  )

  tab1 <- insertTable(src, name = "my_table", table = cars, overwrite = TRUE, temporary = FALSE)
  expect_true(inherits(tab1, "cdm_table"))
  expect_identical(dbplyr::remote_name(tab1), "prefix_my_table")
  expect_identical(omopgenerics::tableName(tab1), "my_table")

  tab2 <- insertTable(src, name = "my_table", table = cars, overwrite = TRUE, temporary = TRUE)
  expect_true(inherits(tab2, "cdm_table"))
  expect_true(is.na(omopgenerics::tableName(tab2)))

  sparklyr::spark_disconnect(con)
  unlink(folder, recursive = TRUE)
})
