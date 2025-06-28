#' Create a `cdm_reference` object from a `sparklyr` connection.
#'
#' @param con A spark connection created with: `sparklyr::spark_connect()`.
#' @param cdmSchema Schema where omop standard tables are located. Schema is
#' defined with a named character list/vector; allowed names are: 'catalog',
#' 'schema' and 'prefix'.
#' @param writeSchema Schema where with writing permissions. Schema is defined
#' with a named character list/vector; allowed names are: 'catalog', 'schema'
#' and 'prefix'.
#' @param cohortTables Names of cohort tables to be read from `writeSchema`.
#' @param cdmVersion The version of the cdm (either "5.3" or "5.4"). If NULL
#' `cdm_source$cdm_version` will be used instead.
#' @param cdmName The name of the cdm object, if NULL
#' `cdm_source$cdm_source_name` will be used instead.
#' @param achillesSchema Schema where achilled tables are located. Schema is
#' defined with a named character list/vector; allowed names are: 'catalog',
#' 'schema' and 'prefix'.
#' @param .softValidation Whether to use soft validation, this is not
#' recommended as analysis pipelines assume the cdm fullfill the validation
#' criteria.
#' @param writePrefix A prefix that will be added to all tables created in the
#' write_schema. This can be used to create namespace in your database
#' write_schema for your tables.
#'
#' @return A cdm reference object
#' @export
#'
#' @examples
#' \dontrun{
#' con <- sparklyr::spark_connect(...)
#' cdmFromSpark(
#'   con = con,
#'   cdmSchema = c(catalog = "...", schema = "...", prefix = "..."),
#'   writeSchema = list() # use `list()`/`c()`/`NULL` to use temporary tables
#' )
#' }
cdmFromSpark <- function(con,
                         cdmSchema,
                         writeSchema,
                         cohortTables = NULL,
                         cdmVersion = NULL,
                         cdmName = NULL,
                         achillesSchema = NULL,
                         .softValidation = FALSE,
                         writePrefix = NULL) {
  # initial checks
  con <- validateConnection(con)
  omopgenerics::assertCharacter(cdmSchema, length = 1, null = FALSE)
  omopgenerics::assertCharacter(writeSchema, length = 1, null = FALSE)
  omopgenerics::assertCharacter(achillesSchema, length = 1, null = TRUE)
  # cdmSchema <- validateSchema(cdmSchema, FALSE)
  # writeSchema <- validateSchema(writeSchema, FALSE)
  # achillesSchema <- validateSchema(achillesSchema, TRUE)
  omopgenerics::assertCharacter(cohortTables, null = TRUE)
  omopgenerics::assertChoice(cdmVersion, c("5.3", "5.4"), length = 1, null = TRUE)
  omopgenerics::assertCharacter(cdmName, length = 1, null = TRUE)
  omopgenerics::assertLogical(.softValidation, length = 1)

  if (.softValidation) {
    cli::cli_inform(c("!" = "Validation has been turned off, this is not recommended as analytical packages assumed the cdm_reference object fulfills the cdm validation criteria."))
  }

  # create spark source
  src <- sparkSource(
    con = con,
    cdmSchema = cdmSchema,
    writeSchema = writeSchema,
    writePrefix = writePrefix
  )

  # available cdm tables
  cdmTables <- sparkListTables(con = con, schema = cdmSchema, prefix = NULL) |>
    purrr::keep(\(x) x %in% omopgenerics::omopTables())

  # extract cdm name
  if (is.null(cdmName)) {
    if ("cdm_source" %in% cdmTables) {
      cdmName <- sparkReadTable(con = con, schema = cdmSchema, prefix = NULL, name = "cdm_source") |>
        dplyr::pull("cdm_source_name")
    }
    if (length(cdmName) != 1) {
      cli::cli_warn(c("!" = "{.pkg cdmName} not found, please provide it using {.arg cdmName} argument."))
      cdmName <- "OMOP"
    }
  }

  # read cdm tables
  cdm <- cdmTables |>
    rlang::set_names() |>
    purrr::map(\(x) {
      sparkReadTable(con = con, schema = cdmSchema, prefix = NULL, name = x) |>
        omopgenerics::newCdmTable(src = src, name = x)
    }) |>
    omopgenerics::newCdmReference(
      cdmName = cdmName,
      cdmVersion = cdmVersion,
      .softValidation = .softValidation
    )

  # read achilles tables
  if (length(achillesSchema) > 0) {
    ls <- sparkListTables(con = con, schema = achillesSchema) |>
      purrr::keep(\(x) x %in% omopgenerics::achillesTables())
    notFound <- omopgenerics::achillesTables() |>
      purrr::keep(\(x) !x %in% ls)
    if (length(notFound) > 0) {
      cli::cli_warn(c("!" = "Achilles tables not found: {notFound}."))
    }
    for (nm in ls) {
      cdm[[nm]] <- sparkReadTable(con = con, schema = achillesSchema, name = nm) |>
        omopgenerics::newCdmTable(src = src, name = nm) |>
        omopgenerics::newAchillesTable()
    }
  }

  # read cohort tables
  cdm <- readCohorts(cdm = cdm, cohortTables = cohortTables, .softValidation = .softValidation)

  cdm
}
readCohorts <- function(cdm, cohortTables, .softValidation) {
  src <- omopgenerics::cdmSource(cdm)
  con <- getCon(src)
  schema <- writeSchema(src)
  prefix <- writePrefix(src)
  ls <- sparkListTables(con = con, schema = schema, prefix = prefix)

  # not found cohorts
  notFound <- cohortTables[!cohortTables %in% ls]
  if (length(notFound) > 0) {
    cli::cli_warn(c("!" = "Not found cohorts: {.pkg {notFound}}."))
  }
  cohortTables <- cohortTables[cohortTables %in% ls]

  for (nm in cohortTables) {
    notPresent <- paste0(nm, c("_set", "_attrition", "_codelist")) |>
      purrr::keep(\(x) !x %in% ls)
    if (length(notPresent) > 0) {
      cli::cli_warn(c("!" = "Attributes: {.var {notPresent}} not found for cohort {.pkg {nm}}."))
    }
    tabs <- list(
      cohort = sparkReadTable(con = con, schema = schema, prefix = prefix, name = nm) |>
        omopgenerics::newCdmTable(src = src, name = nm),
      cohort_set = NULL,
      cohort_attrition = NULL,
      cohort_codelist = NULL
    )
    for (atr in c("set", "attrition", "codelist")) {
      nam <- paste0(nm, "_", atr)
      if (nam %in% ls) {
        tabs[[paste0("cohort_", atr)]] <- sparkReadTable(
          con = con, schema = schema, name = nam, prefix = prefix
        )
      }
    }
    cdm[[nm]] <- tabs$cohort
    cdm[[nm]] <- cdm[[nm]] |>
      omopgenerics::newCohortTable(
        cohortSetRef = tabs$cohort_set,
        cohortAttritionRef = tabs$cohort_attrition,
        cohortCodelistRef = tabs$cohort_codelist,
        .softValidation = .softValidation
      )
  }

  class(cdm) <- c("spark_cdm", class(cdm))

  return(cdm)
}
