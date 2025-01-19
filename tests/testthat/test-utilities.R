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
})
