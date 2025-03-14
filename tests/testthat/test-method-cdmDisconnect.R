test_that("disconnect connection", {
  cdm_local <- omock::mockCdmReference() |>
    omock::mockPerson(nPerson = 10)

  folder <- file.path(tempdir(), omopgenerics::uniqueTableName())
  working_config <- sparklyr::spark_config()
  working_config$spark.sql.warehouse.dir <- folder
  con <- sparklyr::spark_connect(master = "local", config = working_config)
  DBI::dbExecute(con, glue::glue("CREATE SCHEMA IF NOT EXISTS my_schema"))

  src <- sparkSource(con = con, cdmSchema = "my_schema", writeSchema = "my_schema")

  insertCdmTo(cdm_local, src)

  cdm <- cdmFromSpark(
    con = con,
    cdmSchema = "my_schema",
    writeSchema = "my_schema",
    cdmName = "my spark cdm",
    .softValidation = TRUE
  )

  expect_true(sparklyr::connection_is_open(con))
  cdmDisconnect(cdm)
  expect_false(sparklyr::connection_is_open(con))

  unlink(folder, recursive = TRUE)
})

test_that("dropping tables", {
  # TO ADD
  # test that dropping tables with write prefix works
})
