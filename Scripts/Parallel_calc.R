### Parrallel calculation of Hysplit trajectories

### script to calculate the maximum of parallel processes 
### (depends on number of CPU cores), execute them and clean up
## TODO: test rstudioapi::jobRunScript !!!
## TODO: include RAM usage !!!
#!/usr/bin/env Rscript

stopifnot(suppressPackageStartupMessages(require(tidyverse, quietly = TRUE)))
stopifnot(suppressPackageStartupMessages(require(lubridate, quietly = TRUE)))
stopifnot(require(optparse, quietly = TRUE))
source("Scripts/Hysplit.R")

invisible(Sys.setlocale("LC_ALL", locale = "English"))

start_time_calc <- Sys.time()
print_time(paste0("Hysplit parallel calculation started at: ", start_time_calc))

## make an option list for arguments to read in
option_list <- list(
  make_option(c("-f", "--file"), type = "character", default = NULL, 
              help = "dataset file name", metavar = "character"),
  make_option(c("-o", "--output"), type = "character", default = "./Data", 
              help = "output path name [default= %default]", metavar = "character"),
  make_option(c("-m", "--met"), type = "character", default = "./Data/Met_data",
              help = "meterologie data path [default= %default]", metavar = "character"),
  make_option(c("--mettype"), type = "character", default = "gdas1",
              help = "use specific meterologie dataset. (\"gdas1\", \"gfs0p25\") [default= %default]", metavar="character"),
  make_option(c("-b", "--binary"), type = "character", default = "./Hysplit",
              help = "HYSPLIT trajectory model binary folder path", metavar = "character"),
  make_option(c("--mode"), type = "character", default = "ens",
              help = "Trajectory (= traj) or Ensamble (= ens) mode [default= %default]", metavar = "character"),
  make_option(c("--height"), type = "double", default = 500,
              help = "model start height in m [default= %default]", metavar = "double"),
  make_option(c("-i", "--interval"), type = "integer", default = 1,
              help = "trajectory calculation interval in h [default= %default]", metavar = "integer"),
  make_option(c("-d", "--duration"), type = "integer", default = 96,
              help = "trajectory duration calculation in h [default= %default]", metavar = "integer"),
  make_option(c("--direction"), type = "character", default = "backward",
              help = "backward or forward calculation", metavar = "character"),
  make_option(c("--cores"), type = "integer", default = 10,
              help = "how many max cpu-cores should be used? [default= %default]", metavar = "integer"),
  make_option(c("--ssd"), type = "character", default = NULL,
              help = "set ssd path, for faster read in of met data. [default= %default]", metavar = "character")
  ) 

opt_parser <- OptionParser(option_list = option_list)
arguments <- parse_args(opt_parser)
warnings_vec <- vector(mode = "character")

if(is.null(arguments$file)){
  print_help(opt_parser)
  stop("Input file must be supplied!", call. = FALSE)
}

## get table file name
traj_file_name <- 
  str_sub(arguments$file, 
          str_locate_all(arguments$file, 
                         pattern = "/")[[1]][nrow(str_locate_all(arguments$file, pattern = "/")[[1]])] + 1, 
          str_length(arguments$file))

arguments$file <- make_filepath(arguments$file)

if(file.size(arguments$file) <= 0){
  print_help(opt_parser)
  stop(paste0(arguments$file, " is empty or does not exist!"), call. = FALSE)
}
if(!any(arguments$mode == c("ens", "traj"))){
  print_help(opt_parser)
  stop("Wrong model mode supplied! (ens or traj)", call. = FALSE)
}
if(!any(arguments$direction == c("backward", "forward"))){
  print_help(opt_parser)
  stop("Wrong direction supplied! (backward or forward)", call. = FALSE)
}
if(!is.integer(arguments$cores)){
  print_help(opt_parser)
  stop("Need an integer value supplied for max number of CPU cores!", call. = FALSE)
}
if(!(arguments$mettype %in% c("gdas1", "gfs0p25"))) {
  print_help(opt_parser)
  stop("Wrong mettype supplied!", call. = FALSE)
}
if(!dir.exists(arguments$output)){
  stopifnot(dir.create(arguments$output, recursive = TRUE))
  print_time(paste0("Created output dir: ", arguments$output))
}
if(!is.null(arguments$ssd)){
  arguments$ssd <- make_filepath(arguments$ssd)
  if(!dir.exists(arguments$ssd)){
    dir.create(arguments$ssd)
  }
}

## create trajectories calculation directory
output_file_path <- make_filepath(arguments$output)
arguments$output <- paste0(make_filepath(arguments$output), "/Traj_calc")
if(!dir.exists(arguments$output)){
  stopifnot(dir.create(arguments$output))
}

arguments$binary <- make_filepath(arguments$binary)
arguments$met <- make_filepath(arguments$met)
if(!dir.exists(arguments$met)){
  stopifnot(dir.create(arguments$met, recursive = TRUE))
  print_time(paste0("Created meteorology dir: ", arguments$met))
}

## read table in (try different time formats)
read_csv_timeformat <- function(table_file_path) {
  
  calc_csv <- tryCatch({
    read_csv(table_file_path, 
             col_types = list(col_character(), 
                              col_double(), 
                              col_double(), 
                              col_datetime(format = "%d/%m/%Y %H:%M"), 
                              col_datetime(format = "%d/%m/%Y %H:%M")))
  }, warning = function(w) {
    read_csv(table_file_path, 
             col_types = list(col_character(), 
                              col_double(), 
                              col_double(), 
                              col_datetime(format = "%Y-%m-%dT%H:%M:%SZ"), 
                              col_datetime(format = "%Y-%m-%dT%H:%M:%SZ")))
  })
  
  return(calc_csv)
}

calc_csv <- read_csv_timeformat(arguments$file)

## get junks per cpu Cores 
chunks <- arguments$cores
if (chunks > (parallel::detectCores() - 1)) chunks <- (parallel::detectCores() - 1)
if (chunks > nrow(calc_csv)) chunks <- as.numeric(nrow(calc_csv))
if(chunks <= 0) {
  stop("Error in getting the right amount of CPU Cores!", call. = FALSE)
}

## get meterology data beforehand, backward and forward ready
start_time <- min(as_datetime(pull(calc_csv, start_UTC)))
end_time <- max(as_datetime(pull(calc_csv, stop_UTC)))

if(arguments$direction == "backward"){
  start_time <- as_datetime(start_time - dhours(arguments$duration))
} else {
  end_time <- as_datetime(end_time + dhours(arguments$duration))
}

met_function <- paste0("met_", arguments$mettype)
  
met_data <- do.call(eval(met_function), list(start_time = start_time, 
                      end_time = end_time, 
                      download_files = TRUE,
                      path_met_files = paste0(arguments$met, "/")))

rm(start_time, end_time)


## copy met data to ssd
if(!is.null(arguments$ssd)){
  print_time(paste0("Copy Met data to: ", arguments$ssd))
  invisible(file.copy(paste0(arguments$met, "/", met_data), 
            paste0(arguments$ssd, "/"), 
            overwrite = FALSE))
}

## slice input data up into chunks, 
## both floor possible (but maybe not good, because of rounding errors)
for(i in seq_len(chunks)) {    
  chunk_df <- slice(calc_csv, (floor((i-1) * (n()/chunks)) + 1):floor(i * (n()/chunks)))
  write_csv(chunk_df, paste0(arguments$file, "_", i, ".csv"))
  
  ## write hysplit exe for every chunk
  if(!file.exists(paste0(arguments$binary, "/hyts_", arguments$mode, "_", i,".exe"))){
     file.copy(paste0(arguments$binary, "/hyts_", arguments$mode,".exe"), 
              paste0(arguments$binary, "/hyts_", arguments$mode,"_", i,".exe"))
  }
  
  ## write exec_dir for every chunk
  if(!dir.exists(paste0(arguments$output, "/Traj_chunk_", i))){
    dir.create(paste0(arguments$output, "/Traj_chunk_", i))
  }
  rm(chunk_df)
}

## start scripts for every chunk, first one visible as control instance
for(i in chunks:1) {
    system2(command = "Rscript",
            args = c(as.character(make_filepath("/Scripts/Traj_calc.R")),
                     paste0("--file ", arguments$file, "_", i, ".csv"),
                     paste0("--met ", ifelse(is.null(arguments$ssd), arguments$met, arguments$ssd)),
                     paste0("--mettype ", arguments$mettype),
                     paste0("--binary ", arguments$binary, "/hyts_", arguments$mode,"_", i, ".exe "),
                     paste0("--output ", arguments$output, "/Traj_chunk_", i),
                     paste0("--mode ", arguments$mode),
                     paste0("--height ", arguments$height),
                     paste0("--interval ", arguments$interval),
                     paste0("--duration ", arguments$duration),
                     paste0("--direction ", arguments$direction),
                     paste0("--download ", ifelse(is.null(arguments$ssd), TRUE, FALSE))
                     ),
            wait = ifelse(i == 1, TRUE, FALSE),
            stdout = ifelse(i == 1, "", FALSE),
            stderr = ifelse(i == 1, "", FALSE),
            invisible = ifelse(i == 1, FALSE, TRUE))
}


## searchs for all time.txt per chunks and waits, if not all chunks are finished
sleep_time <- 60*2.5
while (length(list.files(arguments$output,
                  pattern = "time.txt",
                  full.names = TRUE,
                  recursive = TRUE,
                  include.dirs = TRUE)) < chunks) {
  print_time(paste0("System waits for all chunks to finish.", " (Sleep for: ", sleep_time, " sec.)"))
  Sys.sleep(sleep_time)
}

## copy and clean up
print_time("Finished Calculation for all Trajectories. System starts to clean up now.")
for (i in seq_len(chunks)) {
  
  if(file.exists(paste0(arguments$file, "_", i, ".csv"))){
    file.remove(paste0(arguments$file, "_", i, ".csv"))
  }
  
  for (k in seq_along(calc_csv$sid)) {
    file.copy(from = paste0(arguments$output, "/Traj_chunk_", i, "/", calc_csv$sid[k], "/"), 
              to = paste0(arguments$output, "/"), 
              recursive = TRUE,
              overwrite = FALSE)
  }
  
  ## deletes folders and ssd met data
  unlink(paste0(arguments$output, "/Traj_chunk_", i), recursive = TRUE)
  if(!is.null(arguments$ssd)){
    for(i in seq_along(met_data)){
      unlink(paste0(arguments$ssd, "/", met_data[i]))
    }
  }
}

## file check for Hysplit calculation errors
file_check <- list.files(paste0(arguments$output), pattern = ".txt", recursive = TRUE, full.names = TRUE)
file_sizes <- as.integer(file.size(file_check))
file_sizes_mean <- mean(file_sizes, na.rm = TRUE)
## +- 5% Fehler
file_sizes_sigma <- 0.05
file_sizes_deviation <- c((file_sizes_mean * (1 - file_sizes_sigma)), (file_sizes_mean * (1 + file_sizes_sigma)))

for(i in seq_along(file_sizes)) {
  
  if(any(c(file_sizes[i] < file_sizes_deviation[1], file_sizes[i] > file_sizes_deviation[2]))) {
    warnings_vec <- rbind(warnings_vec, c(paste0("Discrepancies found in: ", file_check[i])))
    
  }
}

## also check if all trajectories were calculated
file_calculation_check <- function(sid, latitude, longitude, start_UTC, stop_UTC, ...){
  
  dir_sid <- paste0(arguments$output, "/", sid)
  
  if(!dir.exists(dir_sid)){
    warnings_vec <<- rbind(warnings_vec, c(paste0("It seems that entire Sid was not calculated: ", sid)))
    return()
  }
  
  file_check_list <- list.files(dir_sid, pattern = ".txt", recursive = TRUE, full.names = TRUE)
  
  run_time <- as_datetime(start_UTC)
  
  while(difftime(as_datetime(stop_UTC), 
                 as_datetime(run_time), 
                 units = "hours") >= 0){
    
    pattern <- paste0(substr(as.character(date(run_time)), 3, 10),
                      "-",
                      formatC(as.numeric(hour(run_time)), 
                              width = 2, format = "d", flag = "0"))
    
    if(!any(str_detect(file_check_list, pattern))) {
      warnings_vec <<- rbind(warnings_vec, c(paste0("It seems that an entire Trajektory was not calculated: ", pattern, ", from Sid: ", sid)))
    }

    run_time <- run_time + dhours(arguments$interval)
  }
}
     
pwalk(calc_csv, file_calculation_check)
      

## print all info to console and file
print_time(paste0("Clean up finished. All Trajectories are in ", arguments$output))
end_time_calc <- Sys.time()
time_diff <- round(difftime(end_time_calc, start_time_calc, units = "auto"), digits = 2)
print_time(paste0("Finished at: ", end_time_calc))
print_time(paste0("Hysplit calculation time needed: ", time_diff, " ", units(time_diff)))

if(length(warnings_vec) > 0) {
  print_time(paste0(length(warnings_vec), " warnings raised. See warnings.txt for more info"))
}


info_list <- paste0("\nInfo for Hysplit calculation of: \n ", arguments$file, "\n",
                    "Started: ", start_time_calc, "\n",
                    "Ended: ", end_time_calc, "\n",
                    "#Traj: ", length(file_sizes), "\n",
                    "#Cores used: ", chunks, "\n",
                    ".exe used in: ", arguments$binary, "\n",
                    "Mode: ", arguments$mode, "\n",
                    "Starting height: ", arguments$height, " m\n",
                    "Trajectory interval: ", arguments$interval, " h\n",
                    "Trajectory duration: ", arguments$duration, " h\n",
                    "Direction: ", arguments$direction, "\n",
                    "Hysplit calculation time: ", time_diff, " ", units(time_diff),"\n",
                    ifelse(length(warnings_vec) == 0, 
                           "", 
                           paste0(length(warnings_vec), " Warnings raised. See warnings.txt for more info \n")))

cat(info_list, file = paste0(output_file_path, "/info.txt"))
if(length(warnings_vec) > 0) {
  cat(warnings_vec, file = paste0(output_file_path, "/warnings.txt"), append = TRUE)
}
