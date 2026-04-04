tempo_payloads <- function(matrices = character(),
                           language = "ro",
                           last_update = FALSE,
                           split_threshold = 300,
                           chunk_size = 100,
                           verbose = TRUE) {
  url_get_matrix <- "http://statistici.insse.ro:8077/tempo-ins/matrix/"
  
  if (length(matrices) == 0) {
    return(list())
  }
  
  results <- vector("list", length(matrices))
  
  cb_factory <- function(index) {
    force(index)
    function(req) {
      results[[index]] <<- req
    }
  }
  
  pool <- curl::new_pool()
  
  for (i in seq_along(matrices)) {
    url_tempo <- paste0(url_get_matrix, matrices[i])
    curl::curl_fetch_multi(
      url = url_tempo,
      done = cb_factory(i),
      pool = pool
    )
  }
  
  curl::multi_run(pool = pool)
  
  make_chunks <- function(x, chunk_size) {
    split(x, ceiling(seq_along(x) / chunk_size))
  }
  
  normalize_query_piece <- function(x) {
    paste(x, collapse = ",")
  }
  
  build_payload_combinations <- function(parsed_list, mat_code, language,
                                         split_threshold, chunk_size,
                                         last_update) {
    dims_options <- parsed_list$dimensionsMap$options
    
    dim_chunks <- vector("list", length(dims_options))
    
    for (i in seq_along(dims_options)) {
      options_vector <- unlist(dims_options[[i]]$nomItemId, use.names = FALSE)
      
      if (length(options_vector) > split_threshold) {
        dim_chunks[[i]] <- make_chunks(options_vector, chunk_size)
      } else {
        dim_chunks[[i]] <- list(options_vector)
      }
    }
    
    index_grid <- expand.grid(
      lapply(dim_chunks, function(x) seq_along(x)),
      KEEP.OUT.ATTRS = FALSE,
      stringsAsFactors = FALSE
    )
    
    payloads_out <- vector("list", nrow(index_grid))
    
    for (row_idx in seq_len(nrow(index_grid))) {
      enc_parts <- character(length(dim_chunks))
      
      for (dim_idx in seq_along(dim_chunks)) {
        selected_chunk_idx <- index_grid[row_idx, dim_idx]
        selected_values <- dim_chunks[[dim_idx]][[selected_chunk_idx]]
        enc_parts[dim_idx] <- normalize_query_piece(selected_values)
      }
      
      payload_i <- list(
        language = language,
        encQuery = paste(enc_parts, collapse = ":"),
        matCode = mat_code,
        matMaxDim = parsed_list$details$matMaxDim,
        matUMSpec = parsed_list$details$matUMSpec
      )
      
      if (!is.null(parsed_list$details$matRegJ)) {
        payload_i$matRegJ <- parsed_list$details$matRegJ
      }
      
      if (isTRUE(last_update) && !is.null(parsed_list$details$lastUpdate)) {
        payload_i$lastUpdate <- parsed_list$details$lastUpdate
      }
      
      payloads_out[[row_idx]] <- payload_i
    }
    
    payloads_out
  }
  
  results_post <- list()
  
  for (j in seq_along(results)) {
    req <- results[[j]]
    
    if (is.null(req)) {
      warning(sprintf("Matrix request failed for index %d.", j))
      next
    }
    
    parsed_txt <- rawToChar(req$content)
    parsed_list <- jsonlite::fromJSON(parsed_txt)
    mat_code <- sub(".*/", "", req$url)
    
    if (verbose) {
      message(sprintf("Generating payloads for %s ...", mat_code))
    }
    
    payloads_j <- build_payload_combinations(
      parsed_list = parsed_list,
      mat_code = mat_code,
      language = language,
      split_threshold = split_threshold,
      chunk_size = chunk_size,
      last_update = last_update
    )
    
    results_post <- c(results_post, payloads_j)
  }
  
  results_post
}