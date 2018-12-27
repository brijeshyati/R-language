####### https://stackoverflow.com/questions/33206365/plot-hourly-data-using-ggplot2/52666256#52666256


sink("D:\\temp\\phgraphbuilt_13.log")
#### it fetch data from R script "phase_1.r" ##
cat("#########memory consumption #### \n")
memory.limit(size=2400)
cat("############ \n")
rm(list=ls(all=TRUE))

##library(bit64) 
library(data.table)
library(scales)
library(ggplot2)
library(gridExtra)
library(filesstrings)
library(stringr)
library(foreach)
library(snow)
library(doParallel)

singlegraph <- function(directory,ids){
  
  dir1 <- "D:\\temp\\"
  ##ids_dtthrout_DEC15
		##COL_6,keyfield,11/30/2015 23:00,12/1/2015 0:00,12/1/2015 1:00,12/1/2015 2:00,12/1/2015 3:00
		##bri_ram_krishna,MSEDS1,E-3_ecd,0,0,0,0,0

  dir2 <- directory
  # find the files that you want
  listoffiles <- list.files(dir1, "ids_.*\\.*", recursive=FALSE,full.names = TRUE)
  # copy the files to the new folder
  file.copy(listoffiles, dir2)
  print(listoffiles)
  print("completed of raw files from houly to part_1 folder")
  
  #oldmydata <- list.files(path='D:\\temp\\',pattern = "ids_.*\\.*", recursive=FALSE,full.names = TRUE)
  oldmydata <- list.files(path=directory,pattern = "ids_.*\\.*", recursive=FALSE,full.names = TRUE)
  oldmydata <- lapply(oldmydata, data.table::fread)
  ##print(oldmydata,10)
  
  ##################################
  print(ids)
  print("##### filter the file wrt ids ####")
  
  file1 <- oldmydata[[1]][oldmydata[[1]]$COL_6 %in% ids ,]   ### Bw_M
  file2 <- oldmydata[[2]][oldmydata[[2]]$COL_6 %in% ids ,]   ### ava
  file3 <- oldmydata[[3]][oldmydata[[3]]$COL_6 %in% ids ,]   ### Avg(min)
  file4 <- oldmydata[[4]][oldmydata[[4]]$COL_6 %in% ids ,]   ### Avg(Mout)
  
  ##molten.ships <- melt(shipdata, id = c("type","year"))
  file1_1 <- melt(setDT(file1), id.vars = c('COL_6', 'keyfield'))   ### Bw_M
  file2_1 <- melt(setDT(file2), id.vars = c('COL_6', 'keyfield'))   ### ava
  file3_1 <- melt(setDT(file3), id.vars = c('COL_6', 'keyfield'))   ### Avg(min)
  file4_1 <- melt(setDT(file4), id.vars = c('COL_6', 'keyfield'))   ### Avg(Mout)
  
  print(file1_1,10)
  print(file2_1,10)
  print(file3_1,10)
  print(file4_1,10)
  
  #F1 <- c(file1_1,file2_1,file3_1,file4_1)
  
  #### replaceing NA from column "value" with ZERO(0) ###
  
  ### file1_1 ### Bw_M
  ind <-   which(sapply(file1_1, is.numeric))
  for(j in ind){
    set(file1_1, i = which(is.na(file1_1[[j]])), j = j, value = 0)
  }
  file1_1 <- as.data.frame(file1_1) ## converted to data.frame from data.table before building graph
  file1_1$variable <- as.POSIXct(file1_1$variable) ## converted to TIMESTAMP in cha into POSIXct, format:before building graph
  cat("=======================","\n")
  
  ### file2_1### ava
  ind <-   which(sapply(file2_1, is.numeric))
  for(j in ind){
    set(file2_1, i = which(is.na(file2_1[[j]])), j = j, value = 0)
  }
  file2_1 <- as.data.frame(file2_1) ## converted to data.frame from data.table before building graph
  file2_1$variable <- as.POSIXct(file2_1$variable) ## converted to TIMESTAMP in cha into POSIXct, format:before building graph
  cat("=======================","\n")
  
  ### file3_1### Avg(min)
  ind <-   which(sapply(file3_1, is.numeric))
  for(j in ind){
    set(file3_1, i = which(is.na(file3_1[[j]])), j = j, value = 0)
  }
  file3_1 <- as.data.frame(file3_1) ## converted to data.frame from data.table before building graph
  file3_1$variable <- as.POSIXct(file3_1$variable) ## converted to TIMESTAMP in cha into POSIXct, format:before building graph
  cat("=======================","\n")
  
  ### file4_1### Avg(Mout)
  ind <-   which(sapply(file4_1, is.numeric))
  for(j in ind){
    set(file4_1, i = which(is.na(file4_1[[j]])), j = j, value = 0)
  }
  file4_1 <- as.data.frame(file4_1) ## converted to data.frame from data.table before building graph
  file4_1$variable <- as.POSIXct(file4_1$variable) ## converted to TIMESTAMP in cha into POSIXct, format:before building graph
  cat("=======================","\n")
  
  p1 <- ggplot(file1_1,aes(variable,as.integer(value),group=1))+ theme_bw() + geom_line(color = "#00FF00", size = 0.5)+
    scale_x_datetime(labels = date_format("%d:%m; %H", tz = "Asia/Calcutta"), breaks=pretty_breaks(n=40)) +
    theme(axis.text.x = element_text(angle=90,hjust=1)) + geom_point(colour = "red", size = 1) +
    ggtitle("Bw_M")
  
  p2 <- ggplot(file2_1,aes(variable,value,group=1))+ theme_bw() + geom_line(color = "#00AFBB", size = 0.5)+
    scale_x_datetime(labels = date_format("%d:%m; %H", tz = "Asia/Calcutta"), breaks=pretty_breaks(n=40)) +
    theme(axis.text.x = element_text(angle=90,hjust=1)) + geom_point(colour = "red", size = 1) +
    ggtitle("ava")
  
  p3 <- ggplot(file3_1,aes(variable,value,group=1))+ theme_bw() + geom_line(color = "#E7B800", size = 0.5)+
    scale_x_datetime(labels = date_format("%d:%m; %H", tz = "Asia/Calcutta"), breaks=pretty_breaks(n=40)) +
    theme(axis.text.x = element_text(angle=90,hjust=1)) + geom_point(colour = "red", size = 1) +
    ggtitle("Avg(min)")
  
  p4 <- ggplot(file4_1,aes(variable,value,group=1))+ theme_bw() + geom_line(color = "#FC4E07", size = 0.5)+
    scale_x_datetime(labels = date_format("%d:%m; %H", tz = "Asia/Calcutta"), breaks=pretty_breaks(n=40)) +
    theme(axis.text.x = element_text(angle=90,hjust=1)) + geom_point(colour = "red", size = 1) +
    ggtitle("Avg(Mout)") + labs(caption = "(based on data from \"ENT_Bandwidth_Web_Report_Hourly_AS3.csv\" using R)")
  
  ####  grid.arrange(p1,p2,p3,p4)
  
  ggsave(filename = paste0("D:\\temp\\1PHID\\",ids,".png"),grid.arrange(p1,p2,p3,p4) +
           geom_point(size=2, shape=23) + theme_bw(base_size = 10),
         width = 10, height = 8, dpi = 150, units = "in", device='png')
  
}

a <- read.csv("D:\\temp\\p_1.csv",header = T,sep = ",",stringsAsFactors = F)
ids <- a$COL_6
for(i in ids) {
  singlegraph(directory='D:\\temp\\p_1\\',i)
}
#######stopCluster(cl)
sink()
