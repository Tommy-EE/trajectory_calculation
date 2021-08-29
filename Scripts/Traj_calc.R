### Script to calculate all trajectories from a table csv
#!/usr/bin/env Rscript
## Tablehead format: sid, latitude, longitude, start_UTC, stop_UTC
## start_UTC & stop_UTC format: %d/%m/%Y %H:%M

stopifnot(suppressPackageStartupMessages(require(tidyverse, quietly = TRUE)))
stopifnot(suppressPackageStartupMessages(require(lubridate, quietly = TRUE)))
stopifnot(require(optparse, quietly = TRUE))
source("Scripts/Hysplit.R")

## Important, hence the scripts tries to download wrong met data (mär, ... etc)
invisible(Sys.setlocale("LC_ALL", locale = "English"))
## Time to allign all processes
alignment_time <- Sys.time()

## Make Option parser and get arguments from shell script input
option_list <- list(
  make_option(c("-f", "--file"), type = "character", default = NULL, 
              help = "dataset file name", metavar = "character"),
  make_option(c("-o", "--output"), type = "character", default = "./Data/Traj_calc", 
              help = "output path name [default= %default]", metavar = "character"),
  make_option(c("-m", "--met"), type = "character", default = "./Data/Met_data",
              help = "meterologie data path [default= %default]", metavar="character"),
  make_option(c("--mettype"), type = "character", default = "gdas1",
              help = "use specific meterologie dataset. (\"gdas1\", \"gfs0p25\") [default= %default]", metavar="character"),
  make_option(c("-b", "--binary"), type = "character", default = NULL,
              help = "HYSPLIT trajectory model binary", metavar = "character"),
  make_option(c("--mode"), type = "character", default = "ens",
              help = "Trajectory (= traj) or Ensamble (= ens) mode [default= %default]", metavar = "character"),
  make_option(c("--height"), type = "double", default = 500,
              help = "model start height in m [default= %default]", metavar = "double"),
  make_option(c("-i", "--interval"), type = "integer", default = 1,
              help = "trajectory calculation interval in h [default= %default]", metavar = "integer"),
  make_option(c("-d", "--duration"), type = "integer", default = 96,
              help = "trajectory duration calculation in h [default= %default]", metavar = "integer"),
  make_option(c("--direction"), type = "character", default = "backward",
              help = "backward or forward calculation [default= %default]", metavar = "character"),
  make_option(c("--invisible"), type = "logical", default = TRUE,
              help = "invisible hysplit model run? [default= %default]", metavar = "logical"),
  make_option(c("--download"), type = "logical", default = TRUE,
              help = "download met data? [default= %default]", metavar = "logical")
  ) 

opt_parser <- OptionParser(option_list = option_list)
arguments <- parse_args(opt_parser)

if (is.null(arguments$file)){
  print_help(opt_parser)
  stop("Input file must be supplied!", call. = FALSE)
}
if(!any(arguments$mode == c("ens", "traj"))){
  print_help(opt_parser)
  stop("Wrong model mode supplied!", call. = FALSE)
}
if(!any(arguments$direction == c("backward", "forward"))){
  print_help(opt_parser)
  stop("Wrong direction supplied!", call. = FALSE)
}
if(!(arguments$mettype %in% c("gdas1", "gfs0p25"))) {
  print_help(opt_parser)
  stop("Wrong mettype supplied!", call. = FALSE)
}
if(!dir.exists(arguments$output)){
  stopifnot(dir.create(arguments$output, recursive = TRUE))
  print_time(paste0("Created outputfolder: ", arguments$output))
}

arguments$met <- make_filepath(arguments$met)
if(!dir.exists(arguments$met)){
  stopifnot(dir.create(arguments$met, recursive = TRUE))
  print_time(paste0("Created meteorology dir: ", arguments$met))
}

## read table in
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

calc_csv <- read_csv_timeformat(arguments$file) %>%
  rowid_to_column("row_id")

## function to calc the traj from start_UTC to stop_UTC
calc_traj <- function(row_id, sid, latitude, longitude, start_UTC, stop_UTC, ...){
  
  filepath <- paste0(arguments$output, "/", sid, "/")
  filelist <- list.files(filepath, pattern=".txt", 
                         full.names = FALSE, 
                         recursive = TRUE, 
                         ignore.case = TRUE)
  
  run_time <- as_datetime(start_UTC)
  number_calculations <- as.integer(difftime(as_datetime(stop_UTC), 
                                  as_datetime(run_time), 
                                  units = "hours") / arguments$interval)
  procent_value <- 0.1
  i <- 1
  
  if(arguments$invisible == TRUE){
    print_time(paste0("Hysplit calculation started for ", sid," : ", row_id, " / ", nrow(calc_csv))) 
  }
  
  while(difftime(as_datetime(stop_UTC), 
                 as_datetime(run_time), 
                 units = "hours") >= 0){
    
    pattern <- paste0(substr(as.character(date(run_time)), 3, 10),
                      "-",
                      formatC(as.numeric(hour(run_time)), 
                              width = 2, format = "d", flag = "0"))
    
    ## check if calculated trajecotrie already exists, if so skip calculation
    if(sum(str_count(filelist, pattern)) == 0) {
      hysplit(lat = latitude,
              lon = longitude,
              height = arguments$height,
              duration = arguments$duration,
              start_time = as_datetime(run_time),
              direction = arguments$direction,
              mode = arguments$mode,
              exec_dir = as.character(arguments$output),
              met_dir = as.character(arguments$met),
              met_type = as.character(arguments$mettype),
              traj_name = as.character(sid),
              binary_path = arguments$binary,
              invisible_run = arguments$invisible,
              download = arguments$download,
              calc_number = i)
    } else {
      print_time(paste0(run_time, " has already been calculated, skipped calculation."))
    }
    
    i <- i + 1
    if((i / as.numeric(number_calculations)) >= procent_value && arguments$invisible == TRUE) {
      print_time(paste0((procent_value * 100), "% done for ", sid))
      procent_value <- procent_value + 0.1
    }
    run_time <- run_time + dhours(arguments$interval)
  }
  
  if(arguments$invisible == TRUE) {
    print_time(paste0("Calculation finished for ", sid," : ", row_id, " / ", nrow(calc_csv))) 
  }
  
}

## perform calculation function on all rows in input table
pwalk(calc_csv, calc_traj)

## save execution time
alignment_time <- Sys.time() - alignment_time
write(paste0(alignment_time, " ", units(alignment_time)), file = paste0(arguments$output, "/time.txt"))
