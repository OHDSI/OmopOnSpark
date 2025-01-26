
folder <- file.path(tempdir(), "temp_spark")
config <- sparklyr::spark_config()
config$spark.sql.warehouse.dir <- folder

disconnect <- function(con, drop = TRUE) {
  sparklyr::spark_disconnect(con)
  unlink(folder, recursive = TRUE)
}
