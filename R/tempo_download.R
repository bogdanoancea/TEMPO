tempo_download <- function(payloads = NULL,
                           matrices = NULL,
                           path = ".",
                           language = "ro",
                           retries = 3,
                           pause_sec = 1,
                           verbose = TRUE,
                           split_threshold = 300,
                           chunk_size = 100,
                           last_update = FALSE,
                           batch_size = 100) {
  if (is.null(payloads) && is.null(matrices)) {
    stop("Provide either 'payloads' or 'matrices'.")
  }
  
  if (!is.null(payloads) && !is.null(matrices)) {
    warning("Both 'payloads' and 'matrices' were provided. 'payloads' will be used.")
  }
  
  if (is.null(path) || !nzchar(path)) {
    stop("'path' must be a valid directory path.")
  }
  
  if (!dir.exists(path)) {
    dir.create(path, recursive = TRUE, showWarnings = FALSE)
  }
  
  if (!is.numeric(batch_size) || length(batch_size) != 1 || batch_size < 1) {
    stop("'batch_size' must be a positive integer.")
  }
  
  batch_size <- as.integer(batch_size)
  
  if (is.null(payloads)) {
    payloads <- tempo_payloads(
      matrices = matrices,
      language = language,
      last_update = last_update,
      split_threshold = split_threshold,
      chunk_size = chunk_size,
      verbose = verbose
    )
  }
  
  if (!is.list(payloads) || length(payloads) == 0) {
    message("No payloads to process.")
    return(invisible(NULL))
  }
  
  url_csv <- "http://statistici.insse.ro:8077/tempo-ins/pivot"
  total <- length(payloads)
  
  results <- vector("list", total)
  errors <- vector("list", total)
  
  batch_starts <- seq.int(1L, total, by = batch_size)
  n_batches <- length(batch_starts)
  
  if (verbose) {
    message(sprintf(
      "Submitting %d payloads in %d batch(es) of up to %d ...",
      total, n_batches, batch_size
    ))
    pb <- utils::txtProgressBar(min = 0, max = total, style = 3)
    on.exit(close(pb), add = TRUE)
  }
  
  completed_total <- 0L
  
  retry_fetch <- function(payload_json, url, retries, pause_sec) {
    last_error <- NULL
    
    for (attempt in seq_len(retries)) {
      out <- tryCatch({
        h_retry <- build_tempo_handle(payload_json)
        req_retry <- curl::curl_fetch_memory(url, handle = h_retry)
        tempo_logger(req_retry)
        parse_tempo_csv_response(req_retry$content)
      }, error = function(e) {
        last_error <<- e
        NULL
      })
      
      if (!is.null(out)) {
        return(list(result = out, error = NULL))
      }
      
      if (attempt < retries) {
        Sys.sleep(pause_sec * attempt)
      }
    }
    
    list(result = NULL, error = last_error)
  }
  
  for (b in seq_along(batch_starts)) {
    batch_start <- batch_starts[b]
    batch_end <- min(batch_start + batch_size - 1L, total)
    batch_idx <- batch_start:batch_end
    
    if (verbose) {
      message(sprintf(
        "\nBatch %d/%d: payloads %d-%d",
        b, n_batches, batch_start, batch_end
      ))
    }
    
    pool <- curl::new_pool()
    
    for (i in batch_idx) {
      payload_json <- build_tempo_json(payloads[[i]])
      h <- build_tempo_handle(payload_json)
      
      done_callback <- local({
        idx <- i
        function(req) {
          tempo_logger(req)
          
          parsed <- tryCatch({
            parse_tempo_csv_response(req$content)
          }, error = function(e) {
            errors[[idx]] <<- e
            NULL
          })
          
          results[[idx]] <<- parsed
          completed_total <<- completed_total + 1L
          
          if (verbose) {
            utils::setTxtProgressBar(pb, completed_total)
          }
        }
      })
      
      fail_callback <- local({
        idx <- i
        payload_json_local <- payload_json
        
        function(msg) {
          retried <- retry_fetch(
            payload_json = payload_json_local,
            url = url_csv,
            retries = retries,
            pause_sec = pause_sec
          )
          
          results[[idx]] <<- retried$result
          
          if (is.null(retried$result)) {
            if (!is.null(retried$error)) {
              errors[[idx]] <<- retried$error
            } else if (inherits(msg, "error")) {
              errors[[idx]] <<- msg
            } else {
              errors[[idx]] <<- simpleError("Unknown multi request failure.")
            }
          }
          
          completed_total <<- completed_total + 1L
          
          if (verbose) {
            utils::setTxtProgressBar(pb, completed_total)
          }
        }
      })
      
      curl::curl_fetch_multi(
        url = url_csv,
        done = done_callback,
        fail = fail_callback,
        handle = h,
        pool = pool
      )
    }
    
    curl::multi_run(pool = pool)
  }
  
  grouped_results <- list()
  
  for (i in seq_along(payloads)) {
    matcode <- tryCatch(
      get_matcode(payloads[[i]]),
      error = function(e) NA_character_
    )
    
    if (is.na(matcode) || is.null(results[[i]])) {
      next
    }
    
    grouped_results[[matcode]] <- c(grouped_results[[matcode]], list(results[[i]]))
  }
  
  saved_files <- character(0)
  
  for (matcode in names(grouped_results)) {
    res_list <- Filter(Negate(is.null), grouped_results[[matcode]])
    
    if (length(res_list) == 0) {
      next
    }
    
    combined <- do.call(rbind, res_list)
    out_file <- file.path(path, paste0(matcode, ".csv"))
    
    utils::write.csv(combined, out_file, row.names = FALSE)
    saved_files <- c(saved_files, out_file)
    
    if (verbose) {
      message(sprintf("Saved combined file: %s", out_file))
    }
  }
  
  failed_idx <- which(vapply(errors, Negate(is.null), logical(1)))
  
  if (length(failed_idx) > 0) {
    warning(sprintf(
      "%d payload(s) failed. Indices: %s",
      length(failed_idx),
      paste(failed_idx, collapse = ", ")
    ))
  }
  
  message(sprintf(
    "Files are stored in: %s",
    normalizePath(path, winslash = "/", mustWork = FALSE)
  ))
  
  invisible(list(
    files = saved_files,
    results = results,
    errors = errors
  ))
}