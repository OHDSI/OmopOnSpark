
#' Create a `cdm_reference` object from a `sparklyr` connection.
#'
#' @param con A spark connection created with: `sparklyr::spark_connect()`.
#' @param cdmSchema Schema where omop standard tables are located. Schema is
#' defined with a named character list/vector; allowed names are: 'catalog',
#' 'schema' and 'prefix'.
#' @param writeSchema Schema where with writing permissions. Schema is defined
#' with a named character list/vector; allowed names are: 'catalog', 'schema'
#' and 'prefix'.
#' @param achillesSchema Schema where achilled tables are located. Schema is
#' defined with a named character list/vector; allowed names are: 'catalog',
#' 'schema' and 'prefix'.
#' @param tempSchema Schema used to create temporary tables. Schema is
#' defined with a named character list/vector; allowed names are: 'catalog' and
#' 'schema'. 'prefix' will be ignored and all temp tables will be created start
#' with 'tamp_' and 5 random letters.
#' @param cohortTables Names of cohort tables to be read from `writeSchema`.
#' @param cdmVersion The version of the cdm (either "5.3" or "5.4"). If NULL
#' `cdm_source$cdm_version` will be used instead.
#' @param cdmName The name of the cdm object, if NULL
#' `cdm_source$cdm_source_name` will be used instead.
#' @param .softValidation Whether to use soft validation, this is not
#' recommended as analysis pipelines assume the cdm fullfill the validation
#' criteria.
#' @param logSql Path to a folder to write the sql executed. Use `logSql = NULL`
#' if you don't want the sql to be exported.
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
                         achillesSchema = NULL,
                         tempSchema = writeSchema,
                         cohortTables = character(),
                         cdmVersion = NULL,
                         cdmName = NULL,
                         .softValidation = FALSE,
                         logSql = NULL) {
  # initial checks
  con <- validateConnection(con)
  cdmSchema <- validateSchema(cdmSchema, FALSE)
  writeSchema <- validateSchema(writeSchema, FALSE)
  achillesSchema <- validateSchema(achillesSchema, TRUE)
  tempSchema <- validateSchema(tempSchema, FALSE)
  omopgenerics::assertCharacter(cohortTables, null = TRUE)
  omopgenerics::assertChoice(cdmVersion, c("5.3", "5.4"), length = 1, null = T)
  omopgenerics::assertCharacter(cdmName, length = 1, null = TRUE)
  omopgenerics::assertLogical(.softValidation, length = 1)
  logSql <- validateLogSql(logSql)

  if (.softValidation) {
    cli::cli_inform(c("!" = "Validation has been turned off, this is not recommended as analytical packages assumed the cdm_reference object fulfills the cdm validation criteria."))
  }

  # create spark source
  src <- sparkSource(
    con = con,
    writeSchema = writeSchema,
    tempSchema = tempSchema,
    logSql = logSql
  )

  # available cdm tables
  cdmTables <- sparkListTables(con = con, schema = cdmSchema) |>
    purrr::keep(\(x) x %in% omopgenerics::omopTables())

  # extract cdm name
  if (is.null(cdmName)) {
    if ("cdm_source" %in% cdmTables) {
      cdmName <- sparkReadTable(con = con, schema = cdmSchema, name = "cdm_source") |>
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
      sparkReadTable(con = con, schema = cdmSchema, name = x) |>
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

}
readCohorts <- function(cdm, cohortTables, .softValidation) {
  src <- omopgenerics::cdmSource(cdm)
  con <- getCon(src)
  schema <- writeSchema(src)

  ls <- sparkListTables(con = con, schema = schema)

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
      cohort = sparkReadTable(con = con, schema = schema, name = nm) |>
        omopgenerics::newCdmTable(src = src, name = nm),
      cohort_set = NULL,
      cohort_attrition = NULL,
      cohort_codelist = NULL
    )
    for (atr in c("set", "attrition", "codelist")) {
      nam <- paste0(nm, "_", atr)
      if (nam %in% ls) {
        tabs[[paste0("cohort_", atr)]] <- sparkReadTable(
          con = con, schema = schema, name = nam
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

  return(cdm)
}
