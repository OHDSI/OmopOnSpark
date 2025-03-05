#' @export
cdmTableFromSource.spark_cdm <- function(src, value) {
  if (inherits(value, "data.frame")) {
    "To insert a local table to a cdm_reference object use insertTable function." |>
      cli::cli_abort()
  }
  if (!inherits(value, "tbl_lazy")) {
    cli::cli_abort(
      "Can't assign an object of class: {paste0(class(value), collapse = ", ")}
      to a spark cdm_reference object."
    )
  }
  schema <- writeSchema(src)

  remoteName <-  dbplyr::remote_name(value)
  if ("prefix" %in% names(schema)) {
    prefix <- schema$prefix
    if (substr(remoteName, 1, nchar(prefix)) == prefix) {
      remoteName <- substr(remoteName, nchar(prefix) + 1, nchar(remoteName))
    } else {
      cli::cli_warn(c("!" = "The {.var {remoteName}} does have the required prefix."))
      cli::cli_inform(c(i = "Creating a copy in `writeSchema`"))
      value <- sparkComputeTable(query = value, schema = schema, name = remoteName)
    }
  }

  omopgenerics::newCdmTable(table = value, src = src, name = remoteName)
}
