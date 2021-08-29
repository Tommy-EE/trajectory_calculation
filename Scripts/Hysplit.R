## Methods to calculate trajectories with Hysplit

stopifnot(suppressPackageStartupMessages(require(lubridate, quietly = TRUE)))
stopifnot(require(stringr, quietly = TRUE))
stopifnot(require(downloader, quietly = TRUE))

Sys.setlocale("LC_ALL", locale = "English")

## hysplit calculation function
hysplit <- function(lat = NULL,
                    lon = NULL,
                    height = NULL,
                    duration = NULL, # 
                    start_time = NULL, # BEST USE DATE FORMAT: YYYY-MM-DD HH:MM:SS UTC
                    direction = "forward", # forward & backward
                    mode = "traj", # traj & ens
                    vert_motion = 0,
                    model_height = 20000.0,
                    traj_name = NULL,
                    exec_dir = NULL,
                    met_dir = NULL,
                    met_type = NULL,
                    binary_path = NULL,
                    download = TRUE, # To Download MET Data
                    invisible_run = FALSE,
                    calc_number = NULL ) {
  
  stopifnot(mode %in% c("traj", "ens"))
  stopifnot(direction %in% c("forward", "backward"))
  stopifnot(met_type %in% c("gdas1", "gfs0p25"))
  
  
  
  if(is.null(exec_dir)){
    exec_dir <- getwd()
  } else {
    exec_dir <- make_filepath(exec_dir)
  }
  
  if(!dir.exists(exec_dir)){
    stop(paste0("Execution dir does not exists: ", exec_dir) )
  }
  
  if(is.null(met_dir)){
    met_dir <- getwd()
  } else {
    met_dir <- make_filepath(met_dir)
  }
  
  if(is.null(binary_path)) {
    if(mode == "traj"){
      binary_path <-
        make_filepath("/Hysplit/hyts_std.exe")
    }
    if(mode == "ens"){
      binary_path <-
        make_filepath("/Hysplit/hyts_ens.exe")
    }
  } else {
    binary_path <- make_filepath(binary_path)
  }
  
  # Generate name of output folder
  if(is.null(traj_name)) {
    folder_name <- 
      paste0("traj-",
             format(Sys.time(),
                    "%Y-%m-%d-%H-%M-%S"))
  } else {
    folder_name <- traj_name
  }
  
  # Write default versions of the SETUP.CFG and
  # ASCDATA.CFG files in the working directory
  if(file.exists(make_filepath("./Hysplit/ASCDATA.CFG"))) {
    file.copy(make_filepath("./Hysplit/ASCDATA.CFG"), 
              paste0(exec_dir, "/ASCDATA.CFG"),
              overwrite = TRUE)
  } else {
    stop(paste0("Could not find ASCDATA.CFG in ", getwd(), "/Hysplit/ASCDATA.CFG"))
    # hysplit_config_init(dir = exec_dir)
  }
  
  if(file.exists(make_filepath("./Hysplit/SETUP.CFG"))) {
    file.copy(make_filepath("./Hysplit/SETUP.CFG"), 
              paste0(exec_dir, "/SETUP.CFG"),
              overwrite = TRUE)
  } else {
    stop(paste0("Could not find SETUP.CFG in", getwd(), "/Hysplit/SETUP.CFG"))
    # hysplit_config_init(dir = exec_dir)
  }

  if(!file.exists(paste0(exec_dir, "/bdyfiles0p5"))){
    if(file.exists(make_filepath("./Hysplit/bdyfiles0p5/"))) {
      file.copy(make_filepath("./Hysplit/bdyfiles0p5/"), 
                paste0(exec_dir),
                overwrite = TRUE,
                recursive = TRUE)
    } else {
      stop(paste0("Could not find bdyfiles0p5 in", getwd(), "/Hysplit/bdyfiles0p5/"))
      # hysplit_config_init(dir = exec_dir)
    }
  }
  
  ## get start time
  start_time <- as_datetime(start_time)
  
  start_year <- str_sub(year(start_time),3,4)
  
  start_month <- formatC(as.numeric(month(start_time)), 
                         width = 2, format = "d", flag = "0")
  
  start_day <- formatC(as.numeric(day(start_time)), 
                       width = 2, format = "d", flag = "0")
  
  start_hour <- formatC(as.numeric(hour(start_time)), 
                        width = 2, format = "d", flag = "0")
  
  # determine the end time of the model run
  if(direction == "backward") {
    end_time <- as_datetime(start_time - dhours(duration))
  } else {
    end_time <- as_datetime(start_time + dhours(duration))
  }
  
  met <- vector(mode = "character")
  
  ## determine met data and, if needed, download it
  if(met_type == "gdas1") {
    met <- met_gdas1(start_time = start_time, 
                     end_time = end_time,
                     download_files = download,
                     path_met_files = paste0(met_dir, "/"))
  }
  
  if(met_type == "gfs0p25") {
    met <- met_gfs0p25(start_time = start_time, 
                     end_time = end_time,
                     download_files = download,
                     path_met_files = paste0(met_dir, "/"))
    
  }
  
  # construct the output filename string for this model run
  output_filename <-
    paste0(start_year, "-",
           start_month, "-",
           start_day, "-",
           start_hour, "-",
           ifelse(is.null(calc_number), 
                  "", 
                  paste0("#", calc_number, "-")),
           mode,
           ".txt")
  
  # write "CONTROL"
  cat(start_year, " ", 
      start_month, " ",
      start_day, " ",
      start_hour, "\n",
      "1\n",
      lat, " ", 
      lon, " ", 
      height, "\n",
      ifelse(direction == "backward", "-", ""),
      duration, "\n",
      vert_motion, "\n",
      model_height, "\n",
      length(met), "\n",
      file = paste0(exec_dir, "/CONTROL"),
      sep = '', append = FALSE)
  
  # Write met file paths to "CONTROL"
  for(i in seq_along(met)) {
    cat(met_dir, "/\n", met[i], "\n",
        file = paste0(exec_dir, "/CONTROL"),
        sep = '', append = TRUE)
    }
  
  # write path for trajectory output files to "CONTROL"
  cat(exec_dir, "/\n",
      output_filename, "\n",
      file = paste0(exec_dir, "/CONTROL"),
      sep = '', append = TRUE)
  
  # CONTROL file is complete, execute the model run
  # shell(paste0("(cd \"", exec_dir, "\" && \"", binary_path, "\")"), invisible = invisible_run)
  
  save_wd <- getwd()
  setwd(exec_dir)
  system2(command = binary_path,
          stdout = ifelse(invisible_run, FALSE, ""),
          stderr = ifelse(invisible_run, FALSE, ""),
          wait = TRUE,
          invisible = invisible_run)
  
  setwd(save_wd)
  rm(save_wd)
  
  # create the output folder if it doesn't exist
  if(!dir.exists(paste0(exec_dir, "/",
                         folder_name))) {
    dir.create(paste0(exec_dir, "/", folder_name))
  }
  
  # perform the movement of all trajectory files
  # shell(paste0("(cd \"", exec_dir, "\" && move \"", output_filename, "\" \"", paste0(exec_dir, "/", folder_name), "\")"), invisible = invisible_run)
    file.rename(paste0(exec_dir, "/", output_filename), 
                paste0(exec_dir,"/", folder_name ,"/", output_filename))
}

# create default "SETUP.CFG" and "ASCDATA.CFG" files for Hysplit
hysplit_config_init <- function(dir) {
  
  # default `SETUP.CFG` configuration file
  cat(" &SETUP", " tratio = 0.75,", " initd = 0,", " kpuff = 0,", " khmax = 9999,",
      " kmixd = 0,", " kmix0 = 250,", " kzmix = 0,", " kdef = 0,", " kbls = 1,",
      " kblt = 2,", " conage = 48,", " numpar = 2500,", " qcycle = 0.0,", " efile = '',",
      " tkerd = 0.18,", " tkern = 0.18,", " ninit = 1,", " ndump = 1,", " ncycl = 1,",
      " pinpf = 'PARINIT',", " poutf = 'PARDUMP',", " mgmin = 10,", " kmsl = 0,",
      " maxpar = 10000,", " cpack = 1,", " cmass = 0,", " dxf = 1.0,", " dyf = 1.0,",
      " dzf = 0.01,", " ichem = 0,", " maxdim = 1,", " kspl = 1,", " krnd = 6,",
      " frhs = 1.0,", " frvs = 0.01,", " frts = 0.10,", " frhmax = 3.0,", " splitf = 1.0,",
      " tm_pres = 1,", " tm_tpot = 1,", " tm_tamb = 1,", " tm_rain = 1,", " tm_mixd = 1,", 
      " tm_relh = 1,", " tm_dswf = 1,",  
      " /", sep = "\n",
      file = paste0(dir, "/", "SETUP.CFG"))
  
  # default `ASCDATA.CFG` file
  cat("-90.0  -180.0  lat/lon of lower left corner (last record in file)",
      "1.0  1.0	lat/lon spacing in degrees between data points",
      "180  360	lat/lon number of data points",
      "2  	default land use category",
      "0.2  	default roughness length (meters)",
      "'.'  directory location of data files",
      sep = "\n",
      file = paste0(dir, "/", "ASCDATA.CFG"))
}

## get gdas1 meterologie data from NOAA FTP server
## FORM: YEAR = YYYY , MONTH = MM
met_gdas1 <- function(files = NULL,
                      start_time = NULL,
                      end_time = NULL,
                      check_files = TRUE,
                      download_files = TRUE,
                      path_met_files) {
  
  gdas1_ftp <- 
    "ftp://arlftp.arlhq.noaa.gov/archives/gdas1/"
  
  path_met_files <- 
    make_filepath(path_met_files)
  stopifnot(dir.exists(path_met_files))
  
  met <- vector(mode = "character")
  
  # download list of GDAS1 met files by name
  if(!is.null(files)) {
    met <- files
  }
  
  # get met data & download it
  # +/- 3h to determine the right met data
  if(!is.null(start_time) & !is.null(end_time)) {
    months_3_letter <- c("jan", "feb", "mar", "apr", "may", "jun",
                         "jul", "aug", "sep", "oct", "nov", "dec")
    
    stopifnot(difftime(start_time, end_time) != 0)
    
    if(difftime(start_time, end_time) > 0) {
      end_time <- as_datetime(end_time - dhours(3))
      
      met_time <- as_datetime(start_time + dhours(3))
      
      while(difftime(met_time, end_time, units = "days") >= -1){
        met <- cbind(met, paste0("gdas1.", 
                                 months_3_letter[as.numeric(month(met_time))], 
                                 str_sub(year(met_time),3,4),
                                 ".w",
                                 ceiling(day(met_time) / 7)))
        met_time <- met_time - ddays(1)
      }
    } else {
      end_time <- as_datetime(end_time + dhours(3))
      
      met_time <- as_datetime(start_time - dhours(3))
      
      while(difftime(end_time, met_time, units = "days")  >= -1){
        met <- cbind(met, paste0("gdas1.", 
                                 months_3_letter[as.numeric(month(met_time))], 
                                 str_sub(year(met_time),3,4),
                                 ".w",
                                 ceiling(day(met_time) / 7)))
        met_time <- met_time + ddays(1)
      }
    }
  }
  
  met <- union(met,met)
  
  file_sizes_check <- c(171111040, 256666560, 85555520, 598888640)
  
  for(i in seq_along(met)) {
    if((!file.size(paste0(path_met_files, "/", met[i])) %in% file_sizes_check | !check_files) & 
       download_files == TRUE) {
      
      download(url = paste0(gdas1_ftp,
                            met[i]),
               destfile = paste0(path_met_files,
                                 "/",
                                 met[i]),
               method = "auto",
               quiet = FALSE,
               mode = "wb",
               cacheOK = FALSE)
      
      }
  }
  
  return(met)
}

## get gfs0.25 meterologie data from NOAA FTP server
## start_time FORMAT: YEAR = YYYY , MONTH = MM, DAY = DD
## ftp://arlftp.arlhq.noaa.gov/archives/gfs0p25.v1/
met_gfs0p25 <- function(files = NULL,
                      start_time = NULL,
                      end_time = NULL,
                      check_files = TRUE,
                      download_files = TRUE,
                      path_met_files) {
  
  gfs0p25_ftp <- "ftp://arlftp.arlhq.noaa.gov/archives/gfs0p25.v1/"
  
  ## grenze für neuen FTP 20190613_gfs0p25
  break_date_ftp <- ymd("2019-06-13")
  gfs0p25_ftp_new <- "ftp://arlftp.arlhq.noaa.gov/archives/gfs0p25/"
  
  path_met_files <- make_filepath(path_met_files)
  stopifnot(dir.exists(path_met_files))
  
  # download list of GDAS0.25 met files by name
  if(!is.null(files)) {
    met <- vector(mode = "character")
    met <- files
  }
  
  # gfs0.25 format: 20160513_gfs0p25
  # get met data & download it
  # +/- 3h to determine the right met data
  if(!is.null(start_time) & !is.null(end_time)) {
    
    start_time <- as_datetime(start_time)
    end_time <- as_datetime(end_time)
    
    stopifnot(difftime(end_time, start_time) != 0)
    ## swap start and end time if start_time > end_time
    ## to correctly calc +/- 3h
    if(difftime(end_time, start_time) < 0) {
      swap_variable <- start_time
      start_time <- end_time
      end_time <- swap_variable
      rm(swap_variable)
    }
    
    start_time <- as_datetime(start_time - dhours(3))
    end_time <- as_datetime(end_time + dhours(3))
    
    diff_time <- difftime(end_time, start_time, units = "days")
    
    # gfs0.25 format: YYYYMMDD_gfs0p25
    met <- start_time + ddays(0:ceiling(as.numeric(difftime(end_time, start_time), "days")))
    met <- paste0(sprintf("%04i", year(met)), 
                  sprintf("%02i", month(met)), 
                  sprintf("%02i", day(met)),
                  "_gfs0p25")
  }
  
  file_sizes_check <- c(2898905680, 2890599360, 2442058080)
  
  for(i in seq_along(met)) {
    if((!file.size(paste0(path_met_files, "/", met[i])) %in% file_sizes_check | !check_files) & 
       download_files == TRUE) {
      
      url_path <- paste0(ifelse(ymd(str_replace(met[i], "_gfs0p25", "")) >= break_date_ftp, gfs0p25_ftp_new, gfs0p25_ftp)
                          , met[i])
      
      download(url = url_path,
               destfile = paste0(path_met_files,
                                   "/",
                                   met[i]),
               method = "auto",
               quiet = FALSE,
               mode = "wb",
               cacheOK = FALSE)
      
    }
  }
  
  return(met)
}

## make filepaths Hysplit ready, complete paths
## eg. RETURNS: "C:/Documents/trajektorien"
## Full dir path without last backslash "/"
make_filepath <- function(sample_path){
  
  ## check if filepaths starts with "C:/" etc
  check_absolut_path <- function(sample_path){
    if(sum(str_count(sample_path, "^[A-Z]:/")) > 0){
      return(TRUE)
    } else {
      return(FALSE)
    }
  }
  
  ## check if filepaths ends with "/"
  check_end_slash <- function(sample_path){
    if(sum(str_count(sample_path, "/$")) > 0){
      return(TRUE)
    } else {
      return(FALSE)
    }
  }
  
  ## check if filepaths starts with "."
  check_start_point <- function(sample_path){
    if(sum(str_count(sample_path, "^\\./")) > 0){
      return(TRUE)
    } else {
      return(FALSE)
    }
  }
  
  ## start make file path routine
  if(check_end_slash(sample_path) == TRUE){
    sample_path <- strtrim(sample_path,str_length(sample_path)-1)
  }
  
  if(check_absolut_path(sample_path) == FALSE){
    if(check_start_point(sample_path) == TRUE){
      sample_path <- paste0(getwd(), substr(sample_path, 2, str_length(sample_path)))
    } else {
      sample_path <- paste0(getwd(), sample_path)
    }
  }
  return(as.character(sample_path))
}

## Print with SYS TIME
print_time <- function(print_text) {
  print(paste0(format(Sys.time(), "%d-%m-%y %H:%M:%S"), " | ", print_text))
}
