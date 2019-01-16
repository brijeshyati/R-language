
sink("D:\\ZZZZ\\input\\1_log_filegeneration.log")

library(data.table)
library(stringr)



### directory='D:\\ZZZZ\\input\\'
rayaya <- function(directory){
  start <- proc.time()
  #rayaya_yaya1 <- read.csv(file="D:\\ZZZZ\\input\\rayaya_yaya1.csv", header=TRUE, sep=",",stringsAsFactors = FALSE)
  rayaya_yaya1 <- read.csv(file="http://191.xdc.rr.zz/rayaya_yaya1.csv", header=TRUE, sep=",",stringsAsFactors = FALSE)
  rayaya_yaya2 <- read.csv(file="http://191.xdc.rr.zz/rayaya_yaya2.csv", header=TRUE, sep=",",stringsAsFactors = FALSE)
  rayaya_yaya3 <- read.csv(file="http://191.xdc.rr.zz/rayaya_yaya3.csv", header=TRUE, sep=",",stringsAsFactors = FALSE)

  substrRight <- function(x)
  {
    substring="^PPP_|^C_|^NANA_"  ## having 10 characters as 
    ifelse(grepl(substring,x),substr(x, nchar(x)-9, nchar(x)),substr(x, nchar(x)-11, nchar(x)))
    
    ##if (T ==TRUE) { n <- 10
    ##  substr(x, nchar(x)-n+1, nchar(x)) }
    ##else {
    ##  n <- 12
    ##  substr(x, nchar(x)-n+1, nchar(x)) }
    
  } 
  
  
  rayaya_yaya3$IDS <- substrRight(rayaya_yaya3$pname)
  rayaya_yaya31 <- rayaya_yaya3[,c(19,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18)]
  write.table(rayaya_yaya31, file="D:\\ZZZZ\\input\\rayaya_yaya3_newcol.csv",na = 'NA', sep = ',',row.names = F, col.names = T)  
  
  
  fileone <- merge(rayaya_yaya1,rayaya_yaya2,by=c("sdfa","gfh"))
  fileone1 <- fileone[,c(32,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,33,34,35,36,37)]
  
  write.table(fileone1, file="D:\\ZZZZ\\input\\FILE2.csv",na = 'NA', sep = ',',row.names = F, col.names = T)  
  
  
  ##fileoneentele <- merge(fileone1,rayaya_yaya31,by=c("ewq","IDS"))
  fileoneentele <- merge(x=fileone1,y=rayaya_yaya31,by.x='ewq',by.y='IDS',all=TRUE)
  
  fileoneentele1 <- fileoneentele[,c(2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,1,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55)]
  write.table(fileoneentele1, file="D:\\ZZZZ\\input\\FILE3.csv",na = 'NA', sep = ',',row.names = F, col.names = T)    
  
  ##############################################################################
  ##############################################################################
  df_filtered1 <- read.csv(file="D:\\ZZZZ\\input\\FILE3.csv", header=TRUE, sep=",",stringsAsFactors = FALSE)
  
  head(df_filtered1,3)
  df_filtered1[df_filtered1 == ""] <- NA
  #ff = !is.na(df_filtered1$cdsss) || !is.na(df_filtered1$vlnsss)
  
  df_filtered = df_filtered1[!is.na(df_filtered1$cdsss),]
  df_filtered = df_filtered[!is.na(df_filtered$vlnsss),]
  
  df_filtered = df_filtered[grepl("ILL_", df_filtered$cdsss),]
  
  #df_filtered$vlnsss[1]
  #sub("[.].*", ":", df_filtered$vlnsss)[1]
  nms <- c("RRRRR 1","RRRRR 2","RRRRR 3","RRRRR 4")
  kutta <- read.table(text = sub("[.]", ":", df_filtered$vlnsss), sep = ":", as.is = TRUE, col.names = nms)
  
  x =kutta[,2]
  
  substrRight <- function(x, n){
    substr(x, nchar(x)-n+1, nchar(x))
  }
  #substrRight(x, 6)
  
  kutta1 <- paste0(substr(kutta[,2], start=5, stop=12),substrRight(x, 6))
  kutta2 <- paste0("BD",kutta[,4],"_ed")
  kutta3 <- paste0(kutta1,",",kutta2)
  
  kutta <- cbind(kutta,kutta3,stringsAsFactors = FALSE)
  kutta <- kutta[,c("kutta3")]
  
  
  df2 <- cbind(df_filtered,kutta,stringsAsFactors = FALSE)
  
  df2 <- df2[,c("kutta","cdsss","kutta4")]
  
  #df2$kutta4 <- substrRight(df2$kutta4, 5)
  df2$kutta4 <- substr(df2$kutta4,1,nchar(df2$kutta4)-5)
  
  df2 <- unique(df2)
  
  
  #df2 <- transform(df2, end = end + nchar(PATH_sdfa))
  df2 <- setNames(data.frame(df2), c("TTN","oooo_cdsss","WWSSEE"))
  m<- sapply(strsplit(df2$oooo_cdsss, '_'), `[`, 3)
  #JJFFEE =m[[]][3]
  JJFFEE =m
  mmbbff ="" 
  
  df2 <- cbind(df2,JJFFEE,mmbbff,stringsAsFactors = FALSE)
  df2 <- df2[,c(1,2,4,5,3)]
   
  write.table(df2, file="D:\\ZZZZ\\input\\kjmnh.csv",na = 'NA', sep = ',',row.names = F, col.names = T)  
  
}

rayaya(directory='D:\\ZZZZ\\input\\')
sink()
