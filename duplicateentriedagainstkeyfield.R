####### https://stackoverflow.com/questions/33206365/plot-hourly-data-using-ggplot2/52666256#52666256
sink("D:\\temp\\1_log_duplicateentry.log")


library(data.table)
library(scales)
library(ggplot2)
library(gridExtra)
library(reshape)
library(stringr)
library(filesstrings)

### directory='D:\\temp\\'

entduplentry <- function(directory){
  start <- proc.time()
  dir1 <- "D:\\temp\\width\\"
  dir2 <- directory
  # find the files that you want
		
    #BdwhHou.csv_201509250745
		#keyfield,Date(timehourformat),COL_1,COL_2,COL_3,COL_4,COL_5,COL_6,COL_7,COL_8
		#mirayati,25/09/15,06:00,0,0,100,0,0,XXX_YYYY_ZZZZ,ZZZZ,10

  #listoffiles <- list.files(dir1, "BdwhHou.csv.*\\.*", recursive=FALSE,full.names = TRUE)
  listoffiles <- list.files(dir1, "BdwhHou.csv_201812[0-9]{6}", recursive=FALSE,full.names = TRUE)
  
  
  # copy the files to the new folder
  file.copy(listoffiles, dir2)
  
  ## Format number with fixed width and then append .csv to number
  csvfilename <- list.files(path=directory,pattern = "BdwhHou.csv.*\\.*", recursive=FALSE,full.names = TRUE)
  cat("########## step_1",directory,csvfilename,"\n")
  ## Reading in all files and making a large data.table
  lst <- lapply(csvfilename, data.table::fread)
  cat("########## step_1 completed and all the files are in data.table##","\n")
  ##  print(lst)
  cat("########## step_2 rbindlist will be started\n")
  dt <- rbindlist(lst)
  ##  print(dt)
  cat("########## step_2 rbindlist finished\n")
  cat("########################################","\n")
  dt <- setnames(dt, c("KEYFIELD","TIMESTAMP","COL_11","COL_12","COL_13","COL_14","COL_15","COL_16","COL_17","COL_18"))
  dt <- dt[order(dt$'KEYFIELD'),]
  print(dt)
cat("########## step_3 duplicate against ids","\n")
  dupentid <- dt[allDup(dt$COL_16),]  ### function --> handling duplicate ids
  myVector <- c('COL_16', 'KEYFIELD')
  dupentid1 <- dupentid[, myVector,with=FALSE]
  dupentid1 <- dupentid1[!grepl("N_E", dupentid1$COL_16),]  ### removable of N_E details from entcirid column
  dupentid2 <- unique(dupentid1)
  print(dupentid2)
cat("########## step_3 duplicate against ids completed","\n")  
   duplicatecktid <- dcast(setDT(dupentid2), COL_16~rowid(COL_16), value.var="KEYFIELD")
   print(duplicatecktid)
cat("########## step_4 started","\n")     
    write.table(duplicatecktid, file="D:\\temp\\duplicatecktid.csv",na = 'NA', sep = ',',row.names = F, col.names = T)   
cat("########## step_4 duplicate against cktid completed","\n")
    listoffiles1 <- list.files(dir2, "BdwhHou.csv.*\\.*", recursive=FALSE,full.names = TRUE)
    moveprocessedfile(listoffiles1)          ### 6th function -> move processed files
    end <- proc.time()
    print(end - start) # execution time in seconds(to run).
}

    allDup <- function(value){
      duplicated(value) | duplicated(value, fromLast = TRUE)
    }

    moveprocessedfile <- function(listoffiles1){   ### 6th function ###
      file.move(listoffiles1, "D:\\temp\\1_BKP\\",overwrite = TRUE)
    }    

entduplentry(directory='D:\\temp\\')

sink()
