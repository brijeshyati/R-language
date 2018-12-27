####### https://stackoverflow.com/questions/33206365/plot-hourly-data-using-ggplot2/52666256#52666256
### this script will create the files required for report generation (next script)
### this script needs files generatedd from script(duplicateentriedagainstkeyfield.R) 

sink("D:\\temp\\phase_1_HOURLY.log")
cat("#########memory consumption #### \n")
memory.limit(size=2400)
cat("############ \n")
rm(list=ls(all=TRUE))


library(data.table)
library(scales)
library(ggplot2)
library(gridExtra)
library(filesstrings)
library(stringr)

start <- proc.time()

##directory='D:\\temp\\'
phase_1 <- function(directory){
  dir1 <- "D:\\temp\\width\\"
  dir2 <- directory
  # find the files that you want
		#BdwhHou.csv_201509250745
		#keyfield,Date(timehourformat),COL_1,COL_2,COL_3,COL_4,COL_5,COL_6,COL_7,COL_8
		#mirayati,25/09/15,06:00,0,0,100,0,0,XXX_YYYY_ZZZZ,ZZZZ,10
  
  listoffiles <- list.files(dir1, "BdwhHou.csv_201812[0-9]{6}", recursive=FALSE,full.names = TRUE)
  # copy the files to the new folder
  file.copy(listoffiles, dir2)

    f1 <- processingfile(dir2,listoffiles)  ### 1st function ### -> move processed files
    minthrin(f1)      ### 2nd function ###  creation of csv files related to m_in(avg)
    minthrout(f1)     ### 3rd function ###
    mbwidmb(f1)     ### 4th function ###
    mbpsCOL_13(f1)          ### 5th function ###
    
  listoffiles1 <- list.files(dir2, "BdwhHou.csv.*\\.*", recursive=FALSE,full.names = TRUE)
    moveprocessedfile(listoffiles1)          ### 6th function -> move processed files

}
### 1st function ###
processingfile <- function(dir2,listoffiles){ 
  cat("########## processig of function(processingfile) started : step_1",dir2,listoffiles,"\n")
  ## Reading in all files and making a large data.table
  lst <- lapply(listoffiles, data.table::fread)
  cat("########## step_1.1 completed and all the files are in data.table##","\n")
  cat("########## step_2 rbindlist will be started\n")
  dt <- rbindlist(lst)
  cat("########## step_2.1 rbindlist finished\n")
  cat("########################################","\n")
  dt <- setnames(dt, c("KEYFIELD","TIMESTAMP","COL_11","COL_12","COL_13","COL_14","COL_15","COL_16","COL_17","COL_18"))
  dt <- dt[order(dt$KEYFIELD),]
  dt$TIMESTAMP <- as.POSIXct(strptime(dt$TIMESTAMP, "%d/%m/%y, %H:%M")) ### important for publishing of graph
  print(dt)
  cat("#######dt completed ######\\n")
  cat("#################starting of removal of N_E#######################","\n")  
  dt <- dt[!grepl("N_E", dt$COL_16),]
  dt <- unique(dt)
  print(dt)   ### unique print
  str(dt)
  cat("###############completion of removal of N_E#####################","\n")
  return(dt)
}

minthrin <- function(f1) {   ### 2nd function ###
  cat("#################processig of function(minthrin) started ######################","\n")
  dtCOL_14 <- melt(f1, id.vars = c("KEYFIELD","COL_16", "TIMESTAMP"),measure.vars = c("COL_14"))
  print(dtCOL_14)
  str(dtCOL_14)
  dtCOL_141 <- dcast(setDT(dtCOL_14), formula= COL_16+KEYFIELD ~ TIMESTAMP, value.var="value",sum)
  write.table(dtCOL_141, file="D:\\temp\\phase\\HR\\ids_dtCOL_141_DEC15.csv",na = 'NA', sep = ',',row.names = F, col.names = T)  
  print(dtCOL_141)

}

minthrout <- function(f1) {   ### 3rd function ###
  cat("#################processig of function(minthrout) started#######################","\n")
  dtCOL_15 <- melt(f1, id.vars = c("KEYFIELD","COL_16", "TIMESTAMP"),measure.vars = c("COL_15"))
  print(dtCOL_15)
  str(dtCOL_15)
  dtCOL_151 <- dcast(setDT(dtCOL_15), formula= COL_16+KEYFIELD ~ TIMESTAMP, value.var="value",sum)
  write.table(dtCOL_151, file="D:\\temp\\phase\\HR\\ids_dtCOL_151_DEC15.csv",na = 'NA', sep = ',',row.names = F, col.names = T)  
  print(dtCOL_151)
  
}

mbwidmb <- function(f1) {   ### 4th function ###
  cat("################processig of function(mbwidmb) started##########################","\n")
  dtCOL_18 <- melt(f1, id.vars = c("KEYFIELD","COL_16", "TIMESTAMP"),measure.vars = c("COL_18"))
  print(dtCOL_18)
  str(dtCOL_18)
  dtCOL_181 <- dcast(setDT(dtCOL_18), formula= COL_16+KEYFIELD ~ TIMESTAMP, value.var="value",sum)
  write.table(dtCOL_181, file="D:\\temp\\phase\\HR\\ids_dtCOL_181_DEC15.csv",na = 'NA', sep = ',',row.names = F, col.names = T)  
  print(dtCOL_181)
  
}

mbpsCOL_13 <- function(f1) {   ### 5th function ###
  cat("###############processig of function(mbpsCOL_13) started###################","\n")
  dtmbpsCOL_13 <- melt(f1, id.vars = c("KEYFIELD","COL_16", "TIMESTAMP"),measure.vars = c("COL_13"))
  print(dtmbpsCOL_13)
  str(dtmbpsCOL_13)
  dtmbpsCOL_131 <- dcast(setDT(dtmbpsCOL_13), formula= COL_16+KEYFIELD ~ TIMESTAMP, value.var="value",sum)
  write.table(dtmbpsCOL_131, file="D:\\temp\\phase\\HR\\ids_dtmbpsCOL_131_DEC15.csv",na = 'NA', sep = ',',row.names = F, col.names = T)  
  print(dtmbpsCOL_131)
  
}

moveprocessedfile <- function(listoffiles1){   ### 6th function ###
  file.move(listoffiles1, "D:\\temp\\1_BKP\\",overwrite = TRUE)
}


end <- proc.time()
print(end - start) # execution time in seconds(to run).

phase_1(directory='D:\\temp\\') 

sink()
