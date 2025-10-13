test_that("dbplyr", {

  skip_on_cran()

  cars_tibble <- dplyr::as_tibble(cars)

  for(i in seq_along(dbToTest)){

    workingDb <- dbToTest[i]
    cli::cli_alert("Testing dbplyr against {workingDb}")

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

      createOmopTablesOnSpark(con, schemaName = test_schema, cdmVersion = "5.3")

      cdm_spark <- cdmFromSpark(
        con = con,
        cdmName = "test",
        cdmSchema = test_schema,
        writeSchema = test_schema,
        .softValidation = TRUE
      )

      cdm_spark <- insertTable(cdm_spark,
                         name = "cars",
                         table = cars_tibble,
                         overwrite = TRUE)

      expect_equal(cars_tibble |>
                     dplyr::arrange(speed, dist),
                   cdm_spark$cars |>
                     dplyr::collect()  |>
                     dplyr::arrange(speed, dist))
      expect_equal(cars_tibble |>
                     dplyr::arrange(speed, dist),
                   cdm_spark$cars |>
                     dplyr::compute(temporary = TRUE) |>
                     dplyr::collect() |>
                     dplyr::arrange(speed, dist))

      # count records
      expect_equal(cars_tibble |>
                     dplyr::tally() |>
                     dplyr::mutate(n = as.integer(n)),
                   cdm_spark$cars |>
                     dplyr::tally() |>
                     dplyr::collect() |>
                     dplyr::mutate(n = as.integer(n)))
      expect_equal(cars_tibble |>
                     dplyr::count() |>
                     dplyr::mutate(n = as.integer(n)),
                   cdm_spark$cars |>
                     dplyr::count()|>
                     dplyr::collect() |>
                     dplyr::mutate(n = as.integer(n)))
      expect_equal(cars_tibble |>
                     dplyr::summarise(n = dplyr::n()) |>
                     dplyr::mutate(n = as.integer(n)),
                   cdm_spark$cars  |>
                     dplyr::summarise(n = n()) |>
                     dplyr::collect() |>
                     dplyr::mutate(n = as.integer(n)))


      # filter
      expect_equal(cars_tibble |>
                     dplyr::filter(speed == 4) |>
                     dplyr::arrange(speed, dist),
                   cdm_spark$cars  |>
                     dplyr::filter(speed == 4) |>
                     dplyr::collect()|>
                     dplyr::arrange(speed, dist))
      # mutate
      expect_equal(cars_tibble |>
                     dplyr::mutate(new_variable = "a")|>
                     dplyr::arrange(speed, dist),
                   cdm_spark$cars  |>
                     dplyr::mutate(new_variable = "a") |>
                     dplyr::collect()|>
                     dplyr::arrange(speed, dist))
      # select
      expect_equal(sort(cars_tibble |>
                          dplyr::select("speed") |>
                          dplyr::distinct() |>
                          dplyr::pull()),
                   sort(cdm_spark$cars  |>
                          dplyr::select("speed") |>
                          dplyr::distinct() |>
                          dplyr::pull()))

      # count distinct records
      expect_equal(cars_tibble  |>
                     dplyr::distinct() |>
                     dplyr::tally() |>
                     dplyr::mutate(n = as.integer(n)),
                   cdm_spark$cars  |>
                     dplyr::distinct() |>
                     dplyr::tally()|>
                     dplyr::collect() |>
                     dplyr::mutate(n = as.integer(n)))
      expect_equal(cars_tibble  |>
                     dplyr::distinct() |>
                     dplyr::count() |>
                     dplyr::mutate(n = as.integer(n)),
                   cdm_spark$cars |>
                     dplyr::distinct() |>
                     dplyr::count()|>
                     dplyr::collect() |>
                     dplyr::mutate(n = as.integer(n)))
      expect_equal(cars_tibble  |>
                     dplyr::distinct() |>
                     dplyr::summarise(n = dplyr::n()) |>
                     dplyr::mutate(n = as.integer(n)),
                   cdm_spark$cars |>
                     dplyr::distinct() |>
                     dplyr::summarise(n = dplyr::n()) |>
                     dplyr::collect() |>
                     dplyr::mutate(n = as.integer(n)))

      # clean up
      dropSourceTable(cdm_spark, dplyr::everything())
      removeTestSchema(con, test_schema)

      cdmDisconnect(cdm_spark)
    }
  }

})
