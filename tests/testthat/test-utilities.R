test_that("test fullName", {
  schema <- list(catalog = "my_catalog", schema = "my_schema", prefix = "pref")
  expect_identical(fullName(schema, "elephant"), DBI::Id("my_catalog", "my_schema", "prefelephant"))

  schema <- list(prefix = "pref", schema = "my_schema")
  expect_identical(fullName(schema, "elephant"), DBI::Id("my_schema", "prefelephant"))

  schema <- list(schema = "my_schema")
  expect_identical(fullName(schema, "elephant"), DBI::Id("my_schema", "elephant"))

  schema <- list(prefix = "xx_")
  expect_identical(fullName(schema, "elephant"), DBI::Id("xx_elephant"))

  schema <- list()
  expect_identical(fullName(schema, "elephant"), DBI::Id("elephant"))
})

test_that("test validateSchema", {
  writeSchema <- 1
  expect_error(validateSchema(writeSchema))

  writeSchema <- "schema"
  expect_message(res <- validateSchema(writeSchema))
  expect_identical(res, list(schema = "schema"))

  writeSchema <- c(catalog = "schema")
  expect_no_error(res <- validateSchema(writeSchema))
  expect_identical(res, list(catalog = "schema"))

  writeSchema <- list(catalog = "schema")
  expect_no_error(res <- validateSchema(writeSchema))
  expect_identical(res, list(catalog = "schema"))

  writeSchema <- NULL
  expect_no_error(res <- validateSchema(writeSchema))
  expect_identical(res, list())

  writeSchema <- list(catalog = 1)
  expect_error(validateSchema(writeSchema))

  writeSchema <- c("xx", "schema")
  expect_message(res <- validateSchema(writeSchema))
  expect_identical(res, list(catalog = "xx", schema = "schema"))

  writeSchema <- c("xx", "schema", "pf")
  expect_message(res <- validateSchema(writeSchema))
  expect_identical(res, list(catalog = "xx", schema = "schema", prefix = "pf"))

  writeSchema <- c("xx", "schema", "pf", "fghj")
  expect_error(validateSchema(writeSchema))

  writeSchema <- list(catalog = "xx", catalog = "schema")
  expect_error(validateSchema(writeSchema))

  writeSchema <- list(catalog = "xx", not_allowed_name = "schema")
  expect_error(validateSchema(writeSchema))

  writeSchema <- list(catalog = "xx", "schema")
  expect_error(validateSchema(writeSchema))
})

test_that("test checkTemporary", {
  schema1 <- list()
  schema2 <- list(schema = "xxx")
  schema3 <- list(catalog = "xxx", schema = "yyy")
  schema4 <- list(catalog = "xxx", schema = "yyy", prefix = "zzz")
  schema5 <- list(prefix = "zzz")

  expect_identical(checkTemporary(TRUE, schema1), TRUE)
  expect_identical(checkTemporary(TRUE, schema2), TRUE)
  expect_identical(checkTemporary(TRUE, schema3), TRUE)
  expect_identical(checkTemporary(TRUE, schema4), TRUE)
  expect_identical(checkTemporary(TRUE, schema5), TRUE)
  expect_identical(checkTemporary(FALSE, schema1), TRUE)
  expect_identical(checkTemporary(FALSE, schema2), FALSE)
  expect_identical(checkTemporary(FALSE, schema3), FALSE)
  expect_identical(checkTemporary(FALSE, schema4), FALSE)
  expect_identical(checkTemporary(FALSE, schema5), TRUE)
})
