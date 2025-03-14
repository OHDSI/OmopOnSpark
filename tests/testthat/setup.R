
folder <- file.path(tempdir(), "temp_spark")
config <- sparklyr::spark_config()
config$spark.sql.warehouse.dir <- folder

