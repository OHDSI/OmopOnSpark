test_that("sparkListTables works", {
  sc <- mockSparkCon()
  expect_equal(sparklyr::src_databases(sc), "default")
  expect_no_error(sparkListTables(sc))

  sc <- mockSparkCon(con = sc, cdmSchema = list("schema" = "test"))
  expect_equal(sparklyr::src_databases(sc), c("default", "test"))
  expect_no_error(sparkListTables(sc, schema = list("schema" = "test")))

  sparklyr::spark_disconnect(sc)

})

test_that("sparkListTables lists temporary tables", {
  sc <- mockSparkCon()
  df <- data.frame(id = 1:5, value = c("A", "B", "C", "D", "E"))
  sparklyr::sdf_copy_to(sc, df, name = "temp_table", memory = FALSE)
  expect_true("temp_table" %in% sparkListTables(sc))
  sparklyr::spark_disconnect(sc)

})
