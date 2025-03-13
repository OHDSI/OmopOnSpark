test_that("insert permanent table to cdm ref", {

  skip_on_cran()

path <- file.path(tempdir(), "temp_spark")
cdm <- mockSparkCdm(path)

# insert a permanent table
cdm <- insertTable(cdm = cdm,
                   name = "cars",
                   table = cars,
                   overwrite = TRUE,
                   temporary = FALSE)
# we should get back our cdm reference with the table added
expect_true("cars" %in% names(cdm))

expect_identical(
  cdm$cars |>
  dplyr::collect() |>
  dplyr::arrange("speed", "dist"),
  dplyr::tibble(cars) |>
  dplyr::arrange("speed", "dist"))

# insert and overwrite
expect_no_error(insertTable(cdm = cdm,
                         name = "cars",
                         table = cars,
                         overwrite = TRUE,
                         temporary = FALSE))

# overwrite false with existing table - expected error
expect_error(cdm <- insertTable(cdm = cdm,
                   name = "cars",
                   table = cars,
                   overwrite = FALSE,
                   temporary = FALSE))

expect_no_error(cdm <- insertTable(cdm = cdm,
                         name = "cars_2",
                         table = cars,
                         overwrite = FALSE,
                         temporary = FALSE))
expect_true("cars_2" %in% names(cdm))


cdmDisconnect(cdm)
})

test_that("insert temp table to cdm ref", {
# TO ADD
})

test_that("test insert to cdm source", {
  skip_on_cran()

  # create connection
  con <- sparklyr::spark_connect(master = "local", config = config)

  # create schema with prefix
  schema <- list(schema = "my_schema", prefix = "mc_")
  createSchema(con = con, schema = schema)

  # create source
  src <- sparkSource(con = con, writeSchema = schema)

  # insertTable
  tab1 <- insertTable(src, name = "my_table", table = cars, overwrite = TRUE, temporary = FALSE)
  expect_true(inherits(tab1, "cdm_table"))
  expect_identical(dbplyr::remote_name(tab1), "mc_my_table")
  expect_identical(omopgenerics::tableName(tab1), "my_table")
  tab2 <- insertTable(src, name = "my_table", table = cars, overwrite = TRUE, temporary = TRUE)
  expect_true(inherits(tab2, "cdm_table"))
  expect_true(startsWith(dbplyr::remote_name(tab2), "temp_"))
  expect_true(endsWith(dbplyr::remote_name(tab2), "my_table"))
  # TODO expect_true(is.na(omopgenerics::tableName(tab2)))

  sparklyr::spark_disconnect(con)
})
