cdmSchema <- function(src) {
  if (inherits(src, "cdm_reference")) {
    return(attr(omopgenerics::cdmSource(src), "cdm_schema"))
  } else {
    attr(src, "cdm_schema")
  }
}
writeSchema <- function(src) {
  if (inherits(src, "cdm_reference")) {
    return(attr(omopgenerics::cdmSource(src), "write_schema"))
  } else {
    attr(src, "write_schema")
  }
}
writePrefix <- function(src) {
  if (inherits(src, "cdm_reference")) {
    return(attr(omopgenerics::cdmSource(src), "write_prefix"))
  } else {
    attr(src, "write_prefix")
  }
}
getCon <- function(src) {
  if (inherits(src, "cdm_reference")) {
    return(attr(omopgenerics::cdmSource(src), "con"))
  } else {
    attr(src, "con")
  }
}

getWriteTableName <- function(writeSchema, prefix, name) {
  if(!is.null(writeSchema)){
  if (is.null(prefix)) {
    tbl_name <- paste0(writeSchema, ".", name)
  } else {
    tbl_name <- paste0(writeSchema, ".", prefix, name)
  }
  }

  if(is.null(writeSchema)){
    if (is.null(prefix)) {
      tbl_name <- name
    } else {
      tbl_name <- paste0(prefix, name)
    }
  }
  tbl_name
}


