### main script, to start all other scripts
## TODO: make shell stoppable
## TODO: Input and OUTPUT
#!/usr/bin/env Rscript

Sys.setlocale("LC_ALL", locale = "English")

## Tablehead format: sid, latitude, longitude, start_UTC, stop_UTC
## start_UTC & stop_UTC format: %d/%m/%Y %H:%M
file <- "./Data/input/example.csv"       # Table file name  
met <- "./Data/Met_data" # meteorologic data path [default= "./Data/Met_data"]
mettype <- "gdas1"  # "use specific meteorologic dataset. (gdas1, gfs0p25) [default= "gdas1"]"
output <- "./Data"  # output path name [default= "./Data"]
binary <- NULL      # HYSPLIT trajectory model binary [default= "./Hysplit"]
mode <- NULL        # Trajectory (= traj) or Ensemble (= ens) mode [default= "ens"]
height <- NULL      # model start height in m [default= 500]
interval <- NULL    # trajectory calculation interval in h [default= 1]
duration <- NULL    # trajectory duration calculation in h [default= 96]
direction <- NULL   # backward or forward calculation [default = "backward"]
cores <- 2          # how many max cpu-cores should be used? [default = 10]
cutoffs <- TRUE     # calculate additional PBL cutoffs? [default = FALSE]
ssd <- NULL         # set ssd path, for faster read in of met data [default = NULL]

## function for clean forwarding of arguments
direct_arg_forward <- function(arg){
  ifelse(any(is.null(arg),
             is.na(arg),
             arg == ""), return(""), return(paste0("--", deparse(substitute(arg)), " ", arg)))
}

## start script to calculate trajektories from file Table 
## with parallel calculation
system2(command = "Rscript", 
        args = c(paste0(getwd(), "/Scripts/Parallel_calc.R"), 
                 direct_arg_forward(file),
                 direct_arg_forward(output),
                 direct_arg_forward(met),
                 direct_arg_forward(mettype),
                 direct_arg_forward(binary),
                 direct_arg_forward(mode),
                 direct_arg_forward(height),
                 direct_arg_forward(interval),
                 direct_arg_forward(duration),
                 direct_arg_forward(direction),
                 direct_arg_forward(cores),
                 direct_arg_forward(ssd)),
        stdout = "",
        stderr = "",
        wait = TRUE,
        invisible = FALSE)