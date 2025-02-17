test_that("test fullName", {
  schema <- list(catalog = "my_catalog", schema = "my_schema", prefix = "pref")
  expect_identical(fullName(schema, "elephant"), "my_catalog.my_schema.prefelephant")
  expect_identical(fullName(schema, "elephant", TRUE), "`my_catalog`.`my_schema`.`prefelephant`")

  schema <- list(prefix = "pref", schema = "my_schema")
  expect_identical(fullName(schema, "elephant"), "my_schema.prefelephant")

  schema <- list(schema = "my_schema")
  expect_identical(fullName(schema, "elephant"), "my_schema.elephant")

  schema <- list(prefix = "xx_")
  expect_identical(fullName(schema, "elephant"), "xx_elephant")

  schema <- list()
  expect_identical(fullName(schema, "elephant"), "elephant")
})

test_that("test validateSchema", {
  writeSchema <- 1
  expect_error(validateSchema(writeSchema))

  writeSchema <- "schema"
  expect_message(res <- validateSchema(writeSchema))
  expect_identical(res, list(schema = "schema"))

  writeSchema <- c(catalog = "schema")
  expect_error(res <- validateSchema(writeSchema))

  writeSchema <- list(catalog = "schema")
  expect_error(res <- validateSchema(writeSchema))

  writeSchema <- NULL
  expect_error(res <- validateSchema(writeSchema))
  expect_no_error(res <- validateSchema(writeSchema, TRUE))
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
