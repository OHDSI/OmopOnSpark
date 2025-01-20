
con
cdmSchema = NULL
writeSchema = NULL
achillesSchema = NULL
cohortTables = NULL
cdmVersion = NULL
cdmName = NULL
.softValidation = FALSE
logSql = NULL

sparkFullName(schema, name)
sparkListTables(con, schema)
sparkReadTable(con, fullname)
sparkWriteTable(con, fullname, table)
sparkComputeTable(con, fullname, query, temporary)
sparkDropTable(con, fullname) # try if we can make this work with temporary tables

omopSparkSource(con, writeSchema) # class spark_cdm
structure(
  .Data = list(), con = con, write_schema = writeSchema, temporary = is.null(writeSchema)
)

cdmTableFromSource.spark_cdm(src, value)
compute.spark_cdm(x, name, temporary, overwrite) # source <- attr(x, "tbl_source")
dropSourceTable.spark_cdm(src, name)
insertTable.spark_cdm(src, name, table, overwrite, temporary)
insertFromSource.spark_cdm(src, value) # remote_name
listSourceTables.spark_cdm(src)
readSourceTable.spark_cdm(src, name)
