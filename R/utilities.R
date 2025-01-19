fullName <- function(schema, name) {
  DBI::Id(schema$catalog, schema$schema, paste0(schema$prefix, name))
}

validateSchema <- function(schema, con, call = parent.frame()) {
  if (is.null(schema)) {
    return(list())
  }

  # name of object
  nm <- capture.output(substitute(schema))

  # object type
  if (is.character(schema)) {
    schema <- as.list(schema)
  }
  if (!rlang::is_bare_list(schema)) {
    cli::cli_abort(c(x = "{.arg {nm}} must be a named list or character vector."), call = call)
  }

  # check elements are characters of length one
  purrr::imap(schema, \(x, k) {
    if (!is.character(x) || length(x) != 1) {
      cli::cli_abort(c(x = "Elements of {.arg {nm}} must be characters of length 1; element {nm}[[{k}]] is not."), call = call)
    }
  }) |>
    invisible()

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
    } else if (l > 4) {
      cli::cli_abort(c(x = "{nm} can not be have length > 3 (length = {l})."), call = call)
    }
  }

  # object names
  allowedNames <- c("catalog", "schema", "prefix")
  presentNames <- names(schema)
  if (length(presentNames) != length(schema)) {
    cli::cli_abort(c(x = "{.arg {nm}} must be a named list or character vector."), call = call)
  }
  if (length(presentNames) != length(unique(presentNames))) {
    cli::cli_abort(c(x = "Names must be unique in {.arg {nm}}."), call = call)
  }
  if (any(!presentNames %in% allowedNames)) {
    cli::cli_abort(c(x = "Names in {.arg {nm}} must be a choice between {.var {allowedNames}}."), call = call)
  }

  return(schema)
}
