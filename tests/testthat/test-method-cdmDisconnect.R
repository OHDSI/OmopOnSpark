test_that("disconnect connection", {

  cdm_local <- omock::mockCdmReference() |>
    omock::mockPerson(nPerson = 10)

  folder <- file.path(tempdir(), omopgenerics::uniqueTableName())
  working_config <- sparklyr::spark_config()
  working_config$spark.sql.warehouse.dir <- folder
  con <- sparklyr::spark_connect(master = "local", config = working_config)
  createSchema(con = con, schema = list(schema = "my_schema", prefix = "test_"))
  src <- sparkSource(con = con, writeSchema = list(schema = "my_schema", prefix = "test_"))

  insertCdmTo(cdm_local, src)

  cdm <- cdmFromSpark(con = con,
                      cdmSchema = list(schema = "my_schema", prefix = "test_"),
                      writeSchema = list(schema = "my_schema", prefix = "test_"),
                      cdmName = "my spark cdm",
                      .softValidation = TRUE)

  expect_true(sparklyr::connection_is_open(con))
  cdmDisconnect(cdm)
  expect_false(sparklyr::connection_is_open(con))

})

test_that("dropping tables", {
  # TO ADD
  # test that dropping tables with write prefix works


  })
