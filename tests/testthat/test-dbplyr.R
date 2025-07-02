test_that("dbplyr", {
  skip_on_cran()

  if(sparklyr::spark_installed_versions() |> nrow() == 0){
    skip()
  }

  path <- file.path(tempdir(), "temp_dbplyr")
  cdm <- mockSparkCdm(path)

  cars_tibble <- dplyr::as_tibble(cars)

  cdm <- insertTable(cdm,
                     name = "cars",
                     table = cars_tibble,
                     overwrite = TRUE)

  expect_equal(cars_tibble |>
                   dplyr::arrange(speed, dist),
                 cdm$cars |>
                   dplyr::collect()  |>
                   dplyr::arrange(speed, dist))
  expect_equal(cars_tibble |>
                   dplyr::arrange(speed, dist),
                 cdm$cars |>
                   dplyr::compute(temporary = TRUE) |>
                   dplyr::collect() |>
                   dplyr::arrange(speed, dist))

    # count records
    expect_equal(cars_tibble |>
                   dplyr::tally() |>
                   dplyr::mutate(n = as.integer(n)),
                 cdm$cars |>
                   dplyr::tally() |>
                   dplyr::collect() |>
                   dplyr::mutate(n = as.integer(n)))
    expect_equal(cars_tibble |>
                   dplyr::count() |>
                   dplyr::mutate(n = as.integer(n)),
                 cdm$cars |>
                   dplyr::count()|>
                   dplyr::collect() |>
                   dplyr::mutate(n = as.integer(n)))
    expect_equal(cars_tibble |>
                   dplyr::summarise(n = dplyr::n()) |>
                   dplyr::mutate(n = as.integer(n)),
                 cdm$cars  |>
                   dplyr::summarise(n = n()) |>
                   dplyr::collect() |>
                   dplyr::mutate(n = as.integer(n)))


    # filter
    expect_equal(cars_tibble |>
                   dplyr::filter(speed == 4) |>
                   dplyr::arrange(speed, dist),
                 cars_tibble |>
                   dplyr::filter(speed == 4) |>
                   dplyr::collect()|>
                   dplyr::arrange(speed, dist))
    # mutate
    expect_equal(cars_tibble |>
                   dplyr::mutate(new_variable = "a")|>
                   dplyr::arrange(speed, dist),
                 cdm$cars |>
                   dplyr::mutate(new_variable = "a") |>
                   dplyr::collect()|>
                   dplyr::arrange(speed, dist))
    # select
    expect_equal(sort(cars_tibble |>
                        dplyr::select("speed") |>
                        dplyr::distinct() |>
                        dplyr::pull()),
                 sort(cdm$cars |>
                        dplyr::select("speed") |>
                        dplyr::distinct() |>
                        dplyr::pull()))

    # count distinct records
    expect_equal(cars_tibble  |>
                   dplyr::distinct() |>
                   dplyr::tally() |>
                   dplyr::mutate(n = as.integer(n)),
                 cdm$cars |>
                   dplyr::distinct() |>
                   dplyr::tally()|>
                   dplyr::collect() |>
                   dplyr::mutate(n = as.integer(n)))
    expect_equal(cars_tibble  |>
                   dplyr::distinct() |>
                   dplyr::count() |>
                   dplyr::mutate(n = as.integer(n)),
                 cdm$cars |>
                   dplyr::distinct() |>
                   dplyr::count()|>
                   dplyr::collect() |>
                   dplyr::mutate(n = as.integer(n)))
    expect_equal(cars_tibble  |>
                   dplyr::distinct() |>
                   dplyr::summarise(n = dplyr::n()) |>
                   dplyr::mutate(n = as.integer(n)),
                 cdm$cars |>
                   dplyr::distinct() |>
                   dplyr::summarise(n = dplyr::n()) |>
                   dplyr::collect() |>
                   dplyr::mutate(n = as.integer(n)))

    dropSourceTable(cdm = cdm, name = "cars")

    cdmDisconnect(cdm)
    unlink(folder, recursive = TRUE)


  })
