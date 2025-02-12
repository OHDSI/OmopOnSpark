test_that("test internal functions", {
  skip_on_cran()

  # create connection
  con <- sparklyr::spark_connect(master = "local", config = config)

  # create my_schema
  schema <- list(schema = "my_schema")
  createSchema(con = con, schema = schema)

  # no table found at the beginning
  expectTables(con = con, schema = schema, tabs = character())

  # insert table and check it exists
  sparkWriteTable(con = con, schema = schema, name = "cars", value = cars)
  expectTables(con = con, schema = schema, tabs = "cars")

  # read the table
  x <- sparkReadTable(con = con, schema = schema, name = "cars")
  expect_identical(dplyr::as_tibble(cars), dplyr::collect(x))

  # compute to another name
  x |>
    dplyr::mutate(speed_dist = .data$speed * .data$dist) |>
    sparkComputeTable(schema = schema, name = "cars2")
  expectTables(con = con, schema = schema, tabs = c("cars", "cars2"))

  # drop a table
  sparkDropTable(con = con, schema = schema, name = "cars")
  expectTables(con = con, schema = schema, tabs = "cars2")

  # check prefix works
  newSchema <- list(schema = "my_schema", prefix = "mc_")

  # no table found at the beginning
  expectTables(con = con, schema = newSchema, tabs = character())
  expectTables(con = con, schema = schema, tabs = "cars2")

  # insert table and check it exists
  sparkWriteTable(con = con, schema = newSchema, name = "cars", value = cars)
  expectTables(con = con, schema = newSchema, tabs = c("cars"))
  expectTables(con = con, schema = schema, tabs = c("cars2", "mc_cars"))

  # read the table
  x <- sparkReadTable(con = con, schema = newSchema, name = "cars")
  expect_identical(dplyr::as_tibble(cars), dplyr::collect(x))

  # compute to another name
  x |>
    dplyr::mutate(speed_dist = .data$speed * .data$dist) |>
    sparkComputeTable(schema = newSchema, name = "cars2")
  expectTables(con = con, schema = newSchema, tabs = c("cars", "cars2"))
  expectTables(con = con, schema = schema, tabs = c("cars2", "mc_cars", "mc_cars2"))

  # drop a table
  sparkDropTable(con = con, schema = newSchema, name = "cars")
  expectTables(con = con, schema = newSchema, tabs = c("cars2"))
  expectTables(con = con, schema = schema, tabs = c("cars2", "mc_cars2"))

  # disconnect
  disconnect(con)
})

test_that("test source can be created", {
  skip_on_cran()

  # create connection
  con <- sparklyr::spark_connect(master = "local", config = config)

  # use default schema
  schema <- list(schema = "default")
  expectTables(con = con, schema = schema, tabs = character())
  expect_no_error(sparkSource(con = con, writeSchema = schema))
  expectTables(con = con, schema = schema, tabs = character())

  # create schema with prefix
  schema <- list(schema = "my_schema", prefix = "mc_")
  createSchema(con = con, schema = schema)
  expectTables(con = con, schema = schema, tabs = character())
  expect_no_error(sparkSource(con = con, writeSchema = schema))
  expectTables(con = con, schema = schema, tabs = character())

  # disconnect
  disconnect(con)
})

test_that("test methods", {
  # insertTable
  # compute
  # cdmTableFromSource
  # listSourceTables
  # dropSourceTable
  # readSourceTable
  # cdmDisconnect
  # insertCdmTo
})
