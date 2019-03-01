#' Die exportierten Daten von der AIRMO liegen in einer unpraktischen Form für den Import vor:
#' Jeweils mehrere Daten für jeden Parameter. Importiert man diese nun in den Store. Wird bei jedem 
#' neuen Parameter jeweils die bereits importierten Daten gelesen, gemerget und wieder gespeichert.
#' Das heisst die Daten vom ersten Parameter werden ca. 20 mal geschrieben und wieder gelesen.
#' 
#' Von dem her ist es effizienter die Daten zuerst zu transformieren:
#' Lesen der Paremeter Files, splitten nach interval / site und diese Teile dann als RDS speichern. 
#' interval/site/parameter_part_{i}.rds wird als pfad verwendet.
#' 
#' -> Dauer für OL aqmet Daten (<2019): Finished transforming after 516.91 seconds
#' 
#' Den import erledigt man dann als geschachtelten Loop:
#' - für alle interval ordner
#' -- für alle site ordner
#' --- lese alle rds files, hänge die Daten aneinander, speichere sie im store
#' 
#' -> Dauer für OL aqmet Daten (<2019): Finished importing interval 'min30' after 444.70 seconds
#' 
#' Total also 961.61 seconds vs 2159.12 seconds mit rOstluft::import_dir
#' 
#' @examples 
#' path_to_aqmet_dat_files <- "erwDS"
#' path_to_transform_dir <- "transformed"
#' store <- rOstluft::storage_local_rds("aqmet", rOstluft::format_rolf() , read.only = FALSE)
#' 
#' transform_dir(path_to_aqmet_dat_files, path_to_transform_dir)
#' import_transformed(path_to_transform_dir, store)


transform_dir <- function(path, to, ...) {
  dat_files <- fs::dir_ls(path, ...)
  t_transform_start <- Sys.time()
  purrr::map(dat_files, transform_file, to)
  t_transform_end <- Sys.time()
  t_transform <- lubridate::time_length(t_transform_end - t_transform_start, unit = "seconds")
  message(sprintf("Finished transforming after %.2f seconds", t_transform))
}

transform_file <- function(fn, to) {
  t_read_start <- Sys.time()
  data <- rOstluft::read_airmo_dat(fn)
  t_read_end <- Sys.time()
  t_read <- lubridate::time_length(t_read_end - t_read_start, unit = "seconds")
  message(sprintf("Read '%s' in %.2f seconds. Got %d data points",
                  fn, t_read, nrow(data)))
  
  data <- dplyr::group_by(data, .data$interval, .data$site)
  keys <- dplyr::group_keys(data)
  keys <- purrr::transpose(dplyr::mutate_all(keys, as.character))
  data <- dplyr::group_split(data)
  purrr::map2(data, keys, save_part, to)
  t_transformed <- lubridate::time_length(Sys.time() - t_read_end, unit = "seconds")
  message(sprintf("Transformed data to %s in %.2f seconds", to, t_transformed))
}

save_part <- function(data, params, to) {
  site <- base64url::base64_urlencode(params$site)
  site_path <- fs::path(to, params$interval, site)
  parameter <- dplyr::first(data$parameter)
  
  i <- 0
  fn <- stringr::str_c(parameter, "_part_", i, ".rds")
  part_path <- fs::path(site_path, fn)
  
  
  while (fs::file_exists(part_path)) {
    i <- i + 1
    fn <- stringr::str_c(parameter, "_part_", i, ".rds")
    part_path <- fs::path(site_path, fn)
  }
  
  fs::dir_create(site_path)
  saveRDS(data, part_path)
}

import_transformed <- function(path, store) {
  interval_dirs <- fs::dir_ls(path, type = "directory", recursive = FALSE)
  purrr::map(interval_dirs, import_interval, store)
  invisible(NULL)
}

import_interval <- function(path_interval, store) {
  interval <- fs::path_file(path_interval)
  
  site_dirs <- fs::dir_ls(path_interval, type = "directory", recursive = FALSE)
  
  t_interval_start <- Sys.time()
  
  purrr::map(site_dirs, import_site, store)
  
  t_interval<- lubridate::time_length(Sys.time() - t_interval_start, unit = "seconds")
  message(sprintf("Finished importing interval '%s' after %.2f seconds", interval, t_interval))
  
  
  invisible(NULL)
}

import_site <- function(path_site, store) {
  site <- base64url::base64_urldecode(fs::path_file(path_site))
  message(sprintf("Importing '%s'. Start reading files", site))
  t_read_start <- Sys.time()
  
  files <- fs::dir_ls(path_site, type = "file", recursive = FALSE)
  data <- purrr::map(files, readRDS)
  data <- rOstluft::bind_rows_with_factor_columns(!!!data)
  
  t_read_end <- Sys.time()
  t_read <- lubridate::time_length(t_read_end - t_read_start, unit = "seconds")
  message(sprintf("Read '%s' in %.2f seconds. Got %d data points",
                  site, t_read, nrow(data)))
  
  store$put(data)
  t_put_end <- Sys.time()
  t_put <- lubridate::time_length(t_put_end - t_read_end, unit = "seconds")
  message(sprintf("Put data into store %s in %.2f seconds", store$name, t_put))
}


