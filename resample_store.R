# include rOstluft so we fail if it isn't installed on the system. the rest are dependecy from rostluft

library(rOstluft)


#' Resamples one chunk 
#' 
#' This function resample one chunk of a store and puts the result into the store. Optional compares the modification 
#' time of input and output chunk and only resamples if the input chunk is newer.
#'
#' @param fn path to chunk
#' @param store containing the chunk
#' @param new_interval new interval 
#' @param statistics list of statistics see [rOstluft::resample()]
#' @param overwrite overwrite existing data if input is newer. Default FALSE
#'
#' @return content tibble of resampled data
resample_chunk <- function(fn, store, new_interval, statistics, overwrite = FALSE) {
  chunk_name <- fs::path_ext_remove(fs::path_file(fn))
  chunk_vars <- store$format$decode_chunk_name(chunk_name)
  do_resample <- new_interval != chunk_vars$interval
  
  resampled_chunk_name <- store$format$encode_chunk_name(interval = new_interval, site = chunk_vars$site,
                                                         year = chunk_vars$year) 
  fn_resampled_chunk <- store$get_chunk_path(resampled_chunk_name)
  
  if (do_resample && !overwrite && isTRUE(fs::file_exists(fn_resampled_chunk))) {
    chunk_info <- fs::file_info(fn)
    resampled_chunk_info <- fs::file_info(fn_resampled_chunk)
    do_resample <- chunk_info$modification_time > resampled_chunk_info$modification_time    
  }
  
  if (isTRUE(do_resample)) {
    message(sprintf("resampling to %s chunk %s, %s, %s", new_interval, chunk_vars$interval, 
                    chunk_vars$year, chunk_vars$site))  
    data <- store$read_function(fn)
    data_resampled <- rOstluft::resample(data, statistic = statistics, new_interval = new_interval, data.thresh = 0.8, 
                                         rename.parameter = TRUE)
    store$put(data_resampled)
  } else {
    message(sprintf("skip chunk %s, %s, %s", chunk_vars$interval, chunk_vars$year, chunk_vars$site))  
  }
  
}

#' Resamples a store
#' 
#' Resamples the complete data for one interval into an other in a store
#' 
#' @seealso [rOstluft::resample()]
#'
#' @param store instance of to store to resample 
#' @param old_interval input interval
#' @param new_interval output interval
#' @param statistics list of statistics
#' @param overwrite overwrite existing data if input is newer. Default FALSE
#'
#' @return list of content tibble for each chunk
#'
#' @examples
#' store <- rOstluft::storage_local_rds("test_resample", rOstluft::format_rolf(), read.only = FALSE)
#' resample_store(store, "min30", "d1", overwrite = TRUE)
resample_store <- function(store, old_interval, new_interval, statistics = NULL, overwrite = FALSE) {
  if (is.null(statistics)) {
    statistics <- list(
      "RainDur" = "sum",
      "default_statistic" = "mean"
    )
  }
  
  stopifnot(new_interval != old_interval)

  path_data <- fs::path(store$data_path, old_interval)
  file_names <- fs::dir_ls(path_data, recursive = FALSE, type = "file", fail = FALSE)
  purrr::map(file_names, resample_chunk, store, new_interval, statistics, overwrite)
}








