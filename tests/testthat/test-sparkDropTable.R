test_that("sparkDropTable works", {
  con <- mockSparkCon()

  df <- data.frame(id = 1:5, value = c("A", "B", "C", "D", "E"))
  sparklyr::sdf_copy_to(con, df, name = "temp_table", memory = FALSE)
  expect_true("temp_table" %in% DBI::dbListTables(con))
  expect_no_error(sparkDropTable(con = con, tableName = "temp_table", schema = list("schema" = "default")))
  expect_false("temp_table" %in% DBI::dbListTables(con))

  expect_true("measurement" %in% DBI::dbListTables(con))
  expect_no_error(sparkDropTable(con = con, tableName = "measurement", schema = list("schema" = "default")))
  expect_false("measurement" %in% DBI::dbListTables(con))

  sparklyr::spark_disconnect(con)
})
