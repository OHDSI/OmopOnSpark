mockSparkCon <- function(con = sparklyr::spark_connect(master = "local"),
                         databaseName = "GiBleed",
                         schema = NULL){


  validateConnection(con)
  omopgenerics::assertCharacter(databaseName, length = 1)
  schema <- validateSchema(schema)

  # download dataset in temp file
  url <- paste0("https://example-data.ohdsi.dev/", databaseName, ".zip")
  folder <- file.path(tempdir(), paste0("dataset_", paste0(sample(letters, 6), collapse = "")))
  dir.create(folder)
  zipFile <- file.path(folder, "dataset.zip")
  download.file(url, zipFile)

  # unzip dataset
  utils::unzip(zipFile, exdir = folder)

  files <- list.files(paste0(folder, "/", databaseName), full.names = TRUE)

  purrr::map(files, \(x){
    table_name <- tools::file_path_sans_ext(basename(x))

    sparklyr::spark_read_parquet(
      sc = con,
      name = table_name,
      path = x,
      overwrite = TRUE
    )
  })
  return(con)
}
