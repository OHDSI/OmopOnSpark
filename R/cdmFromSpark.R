
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
#' @return
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
                         cdmSchema = list(),
                         writeSchema = list(),
                         achillesSchema = list(),
                         cohortTables = character(),
                         cdmVersion = NULL,
                         cdmName = NULL,
                         .softValidation = FALSE,
                         logSql = NULL) {
  # initial checks
  con <- validateConnection(con)
  cdmSchema <- validateSchema(cdmSchema)
  writeSchema <- validateSchema(writeSchema)
  achillesSchema <- validateSchema(achillesSchema)
  omopgenerics::assertCharacter(cohortTables, null = TRUE)
  omopgenerics::assertChoice(cdmVersion, c("5.3", "5.4"), length = 1, null = T)
  omopgenerics::assertCharacter(cdmName, length = 1, null = TRUE)
  omopgenerics::assertLogical(.softValidation, length = 1)
  logSql <- validateLogSql(logSql)

  if (.softValidation) {
    cli::cli_inform(c("!" = "Validation has been turned off, this is not recommended as analytical packages assumed the cdm_reference object fulfills the cdm validation criteria."))
  }

  # create spark source

  # extract cdm name

  # extract cdm version

  # read cdm tables

  # read achilles tables

  # read cohort tables

}
