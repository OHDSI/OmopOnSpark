test_that("compute working", {

folder <- file.path(tempdir(), "temp_spark_compute")
config <- sparklyr::spark_config()
config$spark.sql.warehouse.dir <- folder
config$sparklyr.gateway.timeout <- 300
options(sparklyr.log.console = TRUE)
con <- sparklyr::spark_connect(master = "local", config = config)
createSchema(con = con, schema = list(schema = "my_schema", prefix = "test_"))
src <- sparkSource(con = con, writeSchema = list(schema = "my_schema", prefix = "test_"))

cdm_local <- omock::emptyCdmReference(cdmName = "mock") |>
  omock::mockPerson(nPerson = 10)

cdm <- insertCdmTo(cdm = cdm_local, to = src)

cdm$person_2 <- cdm$person |>
  dplyr::compute()

expect_true(inherits(cdm$person_2, "cdm_table"))
expect_true(inherits(cdm$person_2, "tbl_spark"))
expect_true(inherits(cdm$person_2, "tbl_sql"))

expect_identical(cdm$person |>
  dplyr::count() |>
  dplyr::collect(),
cdm$person |>
  dplyr::count() |>
  dplyr::compute() |>
  dplyr::collect())

 # original table unaffected
 start_count <- cdm$person |>
    dplyr::tally() |>
   dplyr::pull("n")

  cdm$person_3 <- cdm$person |>
    head(1) |>
    dplyr::compute()

  end_count <- cdm$person |>
    dplyr::tally() |>
    dplyr::pull("n")

  expect_identical(start_count, end_count)

  # overwrite table
  expect_no_error(cdm$person_2 <- cdm$person_2 |>
    head(1) |>
    dplyr::compute(name = "person_2"))


  disconnect(con)

})
