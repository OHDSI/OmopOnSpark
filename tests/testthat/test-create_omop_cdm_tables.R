test_that("sparklyr not supported", {

  folder <- file.path(tempdir(), "temp_spark")
  working_config <- sparklyr::spark_config()
  working_config$spark.sql.warehouse.dir <- folder
  sc <- sparklyr::spark_connect(
   master = "local",
    config = working_config)

 expect_error(create_omop_cdm_tables(sc))

 sparklyr::spark_disconnect(sc)

})
