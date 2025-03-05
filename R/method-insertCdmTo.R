
#' @export
insertCdmTo.spark_cdm <- function(cdm, to) {
  con <- getCon(to)
  writeSchema <- writeSchema(to)

  achillesSchema <- NULL
  cohorts <- character()
  other <- character()
  for (nm in names(cdm)) {
    x <- dplyr::collect(cdm[[nm]])
    cl <- class(x)
    if ("achilles_table" %in% cl) {
      achilles <- writeSchema
    }
    if (!any(c("achilles_table", "omop_table", "cohort_table") %in% cl)) {
      other <- c(other, nm)
    }
    insertTable(cdm = to, name = nm, table = x, overwrite = TRUE)
    if ("cohort_table" %in% cl) {
      cohorts <- c(cohorts, nm)
      insertTable(cdm = to, name = paste0(nm, "_set"), table = attr(x, "cohort_set"), overwrite = TRUE)
      insertTable(cdm = to, name = paste0(nm, "_attrition"), table = attr(x, "cohort_attrition"), overwrite = TRUE)
      insertTable(cdm = to, name = paste0(nm, "_codelist"), table = attr(x, "cohort_codelist"), overwrite = TRUE)
    }
  }

  newCdm <- cdmFromSpark(
    con = con,
    cdmSchema = writeSchema,
    writeSchema = writeSchema,
    achillesSchema = achillesSchema,
    cohortTables = cohorts,
    cdmVersion = omopgenerics::cdmVersion(cdm),
    cdmName = omopgenerics::cdmName(cdm),
    .softValidation = TRUE,
    logSql = logSql(to)
  )

  # newCdm <- omopgenerics::readSourceTable(cdm = newCdm, name = other)

  return(newCdm)
}
