test_that("sparklyr 5.3 - views", {
  skip()
  skip_on_cran()
  if(sparklyr::spark_installed_versions() |> nrow() == 0){
    skip()
  }

  folder <- file.path(tempdir(), "temp_spark")
  working_config <- sparklyr::spark_config()
  working_config$spark.sql.warehouse.dir <- folder
  sc <- sparklyr::spark_connect(
   master = "local",
    config = working_config)

 createOmopTablesOnSpark(sc, schemaName = NULL, cdmVersion = "5.3")
 src <- sparkSource(
   con = sc,
   cdmSchema = NULL,
   writeSchema = NULL
 )
 cdm <- cdmFromSpark(
   con = sc,
   cdmSchema = NULL,
   writeSchema = NULL,
   cdmName = "my spark cdm",
   writePrefix = "study_1_",
   .softValidation = TRUE
 )
 expect_true(omopgenerics::cdmVersion(cdm) == "5.3")
 expect_no_error(omopgenerics::validateCdmArgument(cdm))
 expect_identical(omopgenerics::omopColumns(table = "visit_occurrence", version = "5.3"),
                  colnames(cdm$visit_occurrence))

 sparklyr::spark_disconnect(sc)

})

test_that("sparklyr 5.4 - views", {
  skip()
  skip_on_cran()
  if(sparklyr::spark_installed_versions() |> nrow() == 0){
    skip()
  }

  folder <- file.path(tempdir(), "temp_spark")
  working_config <- sparklyr::spark_config()
  working_config$spark.sql.warehouse.dir <- folder
  sc <- sparklyr::spark_connect(
    master = "local",
    config = working_config)

  createOmopTablesOnSpark(sc, schemaName = NULL, cdmVersion = "5.4")
  src <- sparkSource(
    con = sc,
    cdmSchema = NULL,
    writeSchema = NULL
  )
  cdm <- cdmFromSpark(
    con = sc,
    cdmSchema = NULL,
    writeSchema = NULL,
    cdmVersion = "5.4",
    cdmName = "my spark cdm",
    writePrefix = "study_1_",
    .softValidation = TRUE
  )
  expect_true(omopgenerics::cdmVersion(cdm) == "5.4")
  expect_no_error(omopgenerics::validateCdmArgument(cdm))
  expect_identical(omopgenerics::omopColumns(table = "visit_occurrence", version = "5.4"),
                   colnames(cdm$visit_occurrence))

  sparklyr::spark_disconnect(sc)

})

test_that("odbc 5.3 - delta tables", {

  skip_on_cran()
  skip_on_ci()

  con <- DBI::dbConnect(
    odbc::databricks(),
    httpPath = Sys.getenv("DATABRICKS_HTTPPATH"),
    useNativeQuery = FALSE
  )

  # create schema just for this test
  test_schema <- omopgenerics::uniqueTableName()
  executeQuery(con, glue::glue("CREATE SCHEMA IF NOT EXISTS {test_schema}"))

  src <- sparkSource(
    con = con,
    cdmSchema = test_schema,
    writeSchema = test_schema
  )

  createOmopTablesOnSpark(con, schemaName = test_schema, cdmVersion = "5.3")

  cdm <- cdmFromSpark(
    con = con,
    cdmSchema = test_schema,
    writeSchema = test_schema,
    cdmName = "my spark cdm",
    writePrefix = "study_1_"
  )
  expect_true(omopgenerics::cdmVersion(cdm) == "5.3")
  expect_no_error(omopgenerics::validateCdmArgument(cdm))
  expect_identical(omopgenerics::omopColumns(table = "visit_occurrence", version = "5.3"),
                   colnames(cdm$visit_occurrence))

  # now remove schema and all the tables inside
  executeQuery(con, glue::glue("DROP SCHEMA IF EXISTS {test_schema} CASCADE"))

  DBI::dbDisconnect(con)

})

test_that("odbc 5.4 - delta tables", {

  skip_on_cran()
  skip_on_ci()

  con <- DBI::dbConnect(
    odbc::databricks(),
    httpPath = Sys.getenv("DATABRICKS_HTTPPATH"),
    useNativeQuery = FALSE
  )

  # create schema just for this test
  test_schema <- omopgenerics::uniqueTableName()
  executeQuery(con, glue::glue("CREATE SCHEMA IF NOT EXISTS {test_schema}"))

  src <- sparkSource(
    con = con,
    cdmSchema = test_schema,
    writeSchema = test_schema
  )

  createOmopTablesOnSpark(con, schemaName = test_schema, cdmVersion = "5.4")

  cdm <- cdmFromSpark(
    con = con,
    cdmSchema = test_schema,
    writeSchema = test_schema,
    cdmName = "my spark cdm",
    writePrefix = "study_1_",
    .softValidation = TRUE,
    cdmVersion = "5.4"
  )
  expect_true(omopgenerics::cdmVersion(cdm) == "5.4")
  expect_no_error(omopgenerics::validateCdmArgument(cdm))
  expect_identical(omopgenerics::omopColumns(table = "visit_occurrence", version = "5.4"),
                   colnames(cdm$visit_occurrence))

  # now remove schema and all the tables inside
  executeQuery(con, glue::glue("DROP SCHEMA IF EXISTS {test_schema} CASCADE"))

  DBI::dbDisconnect(con)

})

test_that("odbc 5.4 - prefixed", {

  skip_on_cran()
  skip_on_ci()

  con <- DBI::dbConnect(
    odbc::databricks(),
    httpPath = Sys.getenv("DATABRICKS_HTTPPATH"),
    useNativeQuery = FALSE
  )

  # create schema just for this test
  test_schema <- omopgenerics::uniqueTableName()
  executeQuery(con, glue::glue("CREATE SCHEMA IF NOT EXISTS {test_schema}"))

  src <- sparkSource(
    con = con,
    cdmSchema = test_schema,
    writeSchema = test_schema
  )

  createOmopTablesOnSpark(con, schemaName = test_schema, cdmVersion = "5.4",
                          cdmPrefix = "my_prefix_")

  expect_true("my_prefix_person" %in%
  (DBI::dbGetQuery(con, glue::glue("SHOW TABLES IN {test_schema}")) |> dplyr::pull("tableName")))

  # now remove schema and all the tables inside
  executeQuery(con, glue::glue("DROP SCHEMA IF EXISTS {test_schema} CASCADE"))

  DBI::dbDisconnect(con)

})

test_that("jdbc 5.4 - prefixed", {

  skip_on_cran()
  skip_on_ci()

connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "spark",
  user = Sys.getenv('DATABRICKS_USER'),
  password = Sys.getenv('DATABRICKS_TOKEN'),
  connectionString = paste0(Sys.getenv('DATABRICKS_CONNECTION_STRING'), "EnableArrow=0;")
)
con <- DatabaseConnector::connect(connectionDetails)

# create schema just for this test
test_schema <- omopgenerics::uniqueTableName()
executeQuery(con, glue::glue("CREATE SCHEMA IF NOT EXISTS {test_schema}"))

src <- sparkSource(
  con = con,
  cdmSchema = test_schema,
  writeSchema = test_schema
)

createOmopTablesOnSpark(con, schemaName = test_schema, cdmVersion = "5.4",
                        cdmPrefix = "my_prefix_")

expect_true("my_prefix_person" %in%
              getExistingTables(con, test_schema))

# now remove schema and all the tables inside
executeQuery(con, glue::glue("DROP SCHEMA IF EXISTS {test_schema} CASCADE"))

DBI::dbDisconnect(con)

})
