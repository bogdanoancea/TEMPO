get_matcode <- function(payload) {
  if (!is.null(payload$matCode) && nzchar(payload$matCode)) {
    return(payload$matCode)
  }
  
  if (!is.null(payload$matcode) && nzchar(payload$matcode)) {
    return(payload$matcode)
  }
  
  stop("Payload must contain 'matCode' or 'matcode'.")
}


build_tempo_json <- function(payload) {
  jsonlite::toJSON(
    payload,
    auto_unbox = TRUE,
    pretty = FALSE,
    null = "null"
  )
}


build_tempo_handle <- function(payload_json) {
  h <- curl::new_handle()
  curl::handle_setheaders(h, "Content-Type" = "application/json")
  curl::handle_setopt(
    h,
    customrequest = "POST",
    postfields = payload_json
  )
  h
}


parse_tempo_csv_response <- function(raw_content) {
  txt <- rawToChar(raw_content)
  
  utils::read.csv(
    text = txt,
    sep = ",",
    stringsAsFactors = FALSE,
    check.names = FALSE
  )
}


fetch_tempo_payload <- function(payload_small, url, retries = 3, pause_sec = 1) {
  payload_json <- build_tempo_json(payload_small)
  last_error <- NULL
  
  for (attempt in seq_len(retries)) {
    out <- tryCatch({
      h <- build_tempo_handle(payload_json)
      req <- curl::curl_fetch_memory(url, handle = h)
      tempo_logger(req)
      parse_tempo_csv_response(req$content)
    }, error = function(e) {
      last_error <<- e
      NULL
    })
    
    if (!is.null(out)) {
      return(out)
    }
    
    if (attempt < retries) {
      Sys.sleep(pause_sec * attempt)
    }
  }
  
  stop(sprintf(
    "Request failed after %d attempts: %s",
    retries,
    conditionMessage(last_error)
  ))
}