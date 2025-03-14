test_that("test internal functions", {
  skip_on_cran()

  # create connection
  con <- sparklyr::spark_connect(master = "local", config = config)
  DBI::dbExecute(con, glue::glue("CREATE SCHEMA IF NOT EXISTS results"))

  # no table found at the beginning
  expect_true(length(sparkListTables(con = con,
                                     schema = "results",
                                     prefix = NULL)) == 0)

  # insert table and check it exists
  sparkWriteTable(con = con, schema = "results", prefix = NULL,
                  name = "cars", value = cars)
  expect_true("cars" %in% sparkListTables(con = con,
                  schema = "results",
                  prefix = NULL))

  # read the table
  x <- sparkReadTable(con = con, schema = "results", name = "cars")
  expect_identical(dplyr::as_tibble(cars), dplyr::collect(x))

  # compute to another name
  x |>
    dplyr::mutate(speed_dist = .data$speed * .data$dist) |>
    sparkComputeTable(schema = "results", prefix = NULL, name = "cars2")
  expect_true("cars" %in% sparkListTables(con = con,
                                          schema = "results",
                                          prefix = NULL))
  expect_true("cars2" %in% sparkListTables(con = con,
                                          schema = "results",
                                          prefix = NULL))

  # drop a table
  sparkDropTable(con = con, schema = "results", prefix = NULL, name = "cars")
  expect_true(!"cars" %in% sparkListTables(con = con,
                                          schema = "results",
                                          prefix = NULL))

  sparklyr::spark_disconnect(con)
})


