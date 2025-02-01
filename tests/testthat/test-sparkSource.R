test_that("test internal functions", {
  skip_on_cran()
  # create connection
  con <- sparklyr::spark_connect(master = "local", config = config)

  # check there are no tables
  expect_identical(sparkListTables(con = con, schema = list()), character())

  # insert a temp table and check that we can list it
  x <- cars |>
    utils::head(1) |>
    dplyr::as_tibble()
  sparkWriteTable(con = con, schema = list(), name = "cars", value = x)
  expect_identical(sparkListTables(con = con, schema = list()), "cars")

  # create schema my_schema
  schema <- list(schema = "my_schema")
  createSchema(con = con, schema = schema)

  # write permanent table in my_schema
  sparkWriteTable(con = con, schema = schema, name = "cars2", value = x)
  expect_identical(sparkListTables(con = con, schema = schema), "cars2")

  # expect temp table is still there
  expect_identical(sparkListTables(con = con, schema = list()), "cars")

  # create a new temp table with prefix now
  schema <- list(prefix = "my_prefix_")
  sparkWriteTable(con = con, schema = schema, name = "cars3", value = x)
  expect_identical(sparkListTables(con = con, schema = list()), c("cars", "my_prefix_cars3"))
  expect_identical(sparkListTables(con = con, schema = schema), "cars3")

  # drop only "cars3"
  sparkDropTable(con = con, schema = schema, "cars3")
  expect_identical(sparkListTables(con = con, schema = list()), "cars")
  expect_identical(sparkListTables(con = con, schema = schema), character())

  # read temp table
  df1 <- sparkReadTable(con = con, schema = list(), "cars")
  expect_true(inherits(df1, "tbl_spark"))
  expect_equal(dplyr::collect(df1), x)

  # read permanent
  df2 <- sparkReadTable(con = con, schema = list(schema = "my_schema"), "cars2")
  expect_true(inherits(df2, "tbl_spark"))
  expect_equal(dplyr::collect(df2), x)

  # compute to temporary table
  df1 <- df1 |>
    dplyr::mutate(b = 1L) |>
    sparkComputeTable(schema = list(), name = "cars_modified")
  expect_true("cars_modified" %in% sparkListTables(con, list()))
  expect_true(inherits(df1, "tbl_spark"))
  expect_equal(df1 |> dplyr::collect(), x |> dplyr::mutate(b = 1L))

  schema <- list(schema = "my_schema", prefix = "my_prefix_")
  # compute to permanent table
  df2 <- df2 |>
    dplyr::mutate(x = 1L) |>
    sparkComputeTable(schema = schema, name = "cars_modified")
  expect_true("cars_modified" %in% sparkListTables(con, schema))
  expect_true(inherits(df2, "tbl_spark"))
  expect_equal(df2 |> dplyr::collect(), x |> dplyr::mutate(x = 1L))

  # drop a temporary table
  expect_true("cars_modified" %in% sparkListTables(con, list()))
  sparkDropTable(con, list(), "cars_modified")
  expect_true(!"cars_modified" %in% sparkListTables(con, list()))

  # drop a permanent table
  expect_true("cars_modified" %in% sparkListTables(con, schema))
  sparkDropTable(con, schema, "cars_modified")
  expect_true(!"cars_modified" %in% sparkListTables(con, schema))

  # overwrite a temporary table with write
  schema <- list(prefix = "mc_")
  dft <- sparkWriteTable(con, schema, "cars", cars)
  expect_true(inherits(dft, "tbl_spark"))
  expect_equal(dplyr::collect(dft), dplyr::tibble(cars))
  expect_true("cars" %in% sparkListTables(con, schema))
  expect_true("mc_cars" %in% sparkListTables(con, list()))
  expect_identical(
    dplyr::collect(dft), dplyr::collect(sparkReadTable(con, schema, "cars"))
  )
  dft <- sparkWriteTable(con, schema, "cars", mtcars)
  expect_true(inherits(dft, "tbl_spark"))
  expect_equal(dplyr::collect(dft), dplyr::tibble(mtcars))

  # overwrite a permanent table with write
  schema <- list(schema = "my_schema", prefix = "mc_")
  dft <- sparkWriteTable(con, schema, "cars", cars)
  expect_true(inherits(dft, "tbl_spark"))
  expect_equal(dplyr::collect(dft), dplyr::tibble(cars))
  expect_true("cars" %in% sparkListTables(con, schema))
  expect_true("mc_cars" %in% sparkListTables(con, list()))
  expect_identical(
    dplyr::collect(dft), dplyr::collect(sparkReadTable(con, schema, "cars"))
  )
  dft <- sparkWriteTable(con, schema, "cars", mtcars)
  expect_true(inherits(dft, "tbl_spark"))
  expect_equal(dplyr::collect(dft), dplyr::tibble(mtcars))

  # overwrite a temporary table with compute
  schema <- list()
  expect_true("cars" %in% sparkListTables(con, schema))
  expect_no_error(dft <- sparkComputeTable(dft, schema, "cars"))
  expect_true("cars" %in% sparkListTables(con, schema))

  # overwrite a permanent table with compute
  schema <- list(schema = "my_schema", prefix = "mc_")
  expect_true("cars" %in% sparkListTables(con, schema))
  #expect_no_error(dft <- sparkComputeTable(dft, schema, "cars"))
  expect_true("cars" %in% sparkListTables(con, schema))

  # disconnect
  disconnect(con)
})

test_that("test source can be created", {
  # skip_on_cran()
  # # create connection
  # con <- sparklyr::spark_connect(master = "local", config = config)
  #
  # # no schema provided
  # tbls <- sparkListTables(con = con, schema = list())
  # expect_no_error(sparkSource(con))
  # expect_identical(tbls, sparkListTables(con = con, schema = list()))
  #
  # # no schema provided
  # DBI::dbExecute(conn = con, "CREATE SCHEMA my_schema")
  # schema <- list(schema = "my_schema", prefix = "my_prefix")
  # tbls <- sparkListTables(con = con, schema = schema)
  # tblsTemp <- sparkListTables(con = con, schema = list())
  # expect_no_error(sparkSource(con = con, writeSchema = schema))
  # expect_identical(tbls, sparkListTables(con = con, schema = schema))
  # expect_identical(tblsTemp, sparkListTables(con = con, schema = list()))
  #
  # # disconnect
  # disconnect(con)
})
