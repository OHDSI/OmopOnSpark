fullName <- function(schema, name) {
  DBI::Id(schema$catalog, schema$schema, paste0(schema$prefix, name))
}

validateSchema <- function(schema, call = parent.frame()) {
  if (is.null(schema)) {
    return(list())
  }

  # name of object
  nm <- utils::capture.output(substitute(schema))

  # object type
  if (is.character(schema)) {
    schema <- as.list(schema)
  }
  if (!rlang::is_bare_list(schema)) {
    cli::cli_abort(c(x = "{.arg {nm}} must be a named list or character vector."), call = call)
  }

  # check for unnamed element
  id <- which(names(schema) == "")
  if (length(id) > 0) {
    cli::cli_abort(c(x = "Not named elements found in {nm} (position: {id})."), call = call)
  }

  # check elements are characters of length one
  for (nam in names(schema)) {
    x <- schema[[nam]]
    if (!is.character(x) || length(x) != 1) {
      cli::cli_abort(c(x = "Elements of {.arg {nm}} must be characters of length 1; element {nm}[[{nam}]] is not."), call = call)
    }
  }

  # correct no names
  if (is.null(names(schema))) {
    l <- length(schema)
    if (l == 1) {
      cli::cli_inform(c("!" = "As no names where provided, it was assumed `{nm} = c(schema = '{schema[[1]]}')`"), call = call)
      names(schema) <- "schema"
    } else if (l == 2) {
      cli::cli_inform(c("!" = "As no names where provided, it was assumed `{nm} = c(catalog = '{schema[[1]]}', schema = '{schema[[2]]}')`"), call = call)
      names(schema) <- c("catalog", "schema")
    }  else if (l == 3) {
      cli::cli_inform(c("!" = "As no names where provided, it was assumed `{nm} = c(catalog = '{schema[[1]]}', schema = '{schema[[2]]}', prefix = '{schema[[3]]}')`"), call = call)
      names(schema) <- c("catalog", "schema", "prefix")
    } else if (l > 3) {
      cli::cli_abort(c(x = "{nm} can not be have length > 3 (length = {l})."), call = call)
    }
  }

  # object names
  allowedNames <- c("catalog", "schema", "prefix")
  presentNames <- names(schema)
  if (length(presentNames) != length(unique(presentNames))) {
    cli::cli_abort(c(x = "Names must be unique in {.arg {nm}}."), call = call)
  }
  notAllowed <- presentNames[!presentNames %in% allowedNames]
  if (length(notAllowed) > 0) {
    cli::cli_abort(c(x = "Names in {.arg {nm}} must be a choice between {.var {allowedNames}}. Not allowed names found: {.var {notAllowed}}."), call = call)
  }

  return(schema)
}
validateConnection <- function(con, call = parent.frame()) {
  if (!inherits(con, "spark_connection")) {
    cli::cli_abort(c(x = "{.arg con} must a {.cls spark_connection} object."), call = call)
  }
  if (!sparklyr::connection_is_open(con)) {
    cli::cli_abort(c(x = "{.arg con} connection is closed, please provide an open connection."), call = call)
  }
  return(con)
}
validateLogSql <- function(logSql, call = parent.frame()) {
  omopgenerics::assertCharacter(logSql, length = 1, null = TRUE, call = call)
  if (!is.null(logSql)) {
    if (!dir.exists(logSql)) {
      cli::cli_abort(c("!" = "Directory {.path {logSql}} does not exist."), call = call)
    }
  }
  return(logSql)
}
