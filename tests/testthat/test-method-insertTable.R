test_that("insertTable", {

  skip_on_cran()

  for(i in seq_along(dbToTest)){

    workingDb <- dbToTest[i]
    cli::cli_alert("Testing insertTable() against {workingDb}")

    if(workingDb == "sparklyr") {
      folder <- file.path(tempdir(), "temp_spark")
      con <-  getTestCon("sparklyr", folder)
      test_schema <- NULL
    } else if(workingDb == "odbc") {
      con <-  getTestCon("odbc")
      test_schema <- omopgenerics::uniqueTableName()
    } else if (workingDb == "jdbc") {
      con <-  getTestCon("jdbc")
      test_schema <- omopgenerics::uniqueTableName()
    } else{
      cli::cli_abort("{workingDb} not supported")
    }

    if(is.null(con)){
      cli::cli_inform(" - Skipping tests for {workingDb}")
    } else {
      createEmptyTestSchema(con, test_schema)

      createOmopTablesOnSpark(con, schemaName = test_schema, cdmVersion = "5.3", cdmPrefix = "test_")

      cdm <- cdmFromSpark(
        con = con,
        cdmSchema = test_schema,
        writeSchema = test_schema,
        cdmPrefix = "test_",
        cdmName = "my spark cdm",
        writePrefix = "test_",
        .softValidation = TRUE
      )

      # insert a permanent table
      cdm <- insertTable(
        cdm = cdm,
        name = "cars",
        table = cars,
        overwrite = TRUE,
        temporary = FALSE
      )
      # we should get back our cdm reference with the table added
      expect_true("cars" %in% names(cdm))
      expect_identical(omopgenerics::tableName(cdm$cars), "cars")
      expect_no_error(omopgenerics::validateCdmTable(cdm$cars))

      expect_identical(
        cdm$cars |>
          dplyr::collect() |>
          dplyr::arrange("speed", "dist"),
        dplyr::tibble(cars) |>
          dplyr::arrange("speed", "dist")
      )

      # will be in write schema and use write prefix
      expect_identical(
        dplyr::tbl(
          getCon(cdm),
          I(paste0(writePrefix(cdm), "cars"))
        ) |>
          dplyr::collect() |>
          dplyr::arrange("speed", "dist"),
        dplyr::tibble(cars) |>
          dplyr::arrange("speed", "dist")
      )
      # will not be able to reference the table without the prefix
      expect_error(dplyr::tbl(
        getCon(cdm),
        I(paste0("cars"))
      ))

      # insert and overwrite
      expect_no_error(insertTable(
        cdm = cdm,
        name = "cars",
        table = cars,
        overwrite = TRUE,
        temporary = FALSE
      ))

      # overwrite false with existing table - expected error
      expect_error(cdm <- insertTable(
        cdm = cdm,
        name = "cars",
        table = cars,
        overwrite = FALSE,
        temporary = FALSE
      ))

      expect_no_error(cdm <- insertTable(
        cdm = cdm,
        name = "cars_2",
        table = cars,
        overwrite = FALSE,
        temporary = FALSE
      ))
      expect_true("cars_2" %in% names(cdm))


      # insert a temp table
      cdm <- insertTable(
        cdm = cdm,
        name = "cars",
        table = cars,
        overwrite = TRUE,
        temporary = TRUE
      )
      # we should get back our cdm reference with the table added
      expect_true("cars" %in% names(cdm))
      expect_true(is.na(omopgenerics::tableName(cdm$cars)))
      expect_no_error(omopgenerics::validateCdmTable(cdm$cars))

      expect_identical(
        cdm$cars |>
          dplyr::collect() |>
          dplyr::arrange("speed", "dist"),
        dplyr::tibble(cars) |>
          dplyr::arrange("speed", "dist")
      )

      # insert and overwrite
      expect_no_error(insertTable(
        cdm = cdm,
        name = "cars",
        table = cars,
        overwrite = TRUE,
        temporary = TRUE
      ))

      # overwrite false with existing table
      # this will no longer cause an error when using temp tables
      expect_no_error(cdm <- insertTable(
        cdm = cdm,
        name = "cars_tmp",
        table = cars,
        overwrite = FALSE,
        temporary = TRUE
      ))
      expect_no_error(cdm <- insertTable(
        cdm = cdm,
        name = "cars_tmp",
        table = cars,
        overwrite = FALSE,
        temporary = TRUE
      ))
      expect_true("cars_tmp" %in% names(cdm))

      # test insert to cdm source - no prefix
      src <- sparkSource(
        con = con,
        cdmSchema = NULL,
        writeSchema = NULL,
        writePrefix = NULL
      )

      tab1 <- insertTable(src, name = "my_table", table = cars, overwrite = TRUE, temporary = FALSE)
      expect_true(inherits(tab1, "cdm_table"))
      expect_identical(dbplyr::remote_name(tab1), "my_table")
      expect_identical(omopgenerics::tableName(tab1), "my_table")

      tab2 <- insertTable(src, name = "my_table", table = cars, overwrite = TRUE, temporary = TRUE)
      expect_true(inherits(tab2, "cdm_table"))
      expect_true(is.na(omopgenerics::tableName(tab2)))

     # test insert to cdm source - prefix
      src <- sparkSource(
        con = con, cdmSchema = NULL,
        writeSchema = NULL, writePrefix = "prefix_"
      )

      tab1 <- insertTable(src, name = "my_table", table = cars, overwrite = TRUE, temporary = FALSE)
      expect_true(inherits(tab1, "cdm_table"))
      expect_identical(dbplyr::remote_name(tab1), "prefix_my_table")
      expect_identical(omopgenerics::tableName(tab1), "my_table")

      tab2 <- insertTable(src, name = "my_table", table = cars, overwrite = TRUE, temporary = TRUE)
      expect_true(inherits(tab2, "cdm_table"))
      expect_true(is.na(omopgenerics::tableName(tab2)))


      # clean up
      dropSourceTable(cdm, dplyr::everything())
      dropSourceTable(src, dplyr::everything())
      removeTestSchema(con, test_schema)

      cdmDisconnect(cdm)
    }
  }

})
