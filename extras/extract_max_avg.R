library(jsonlite)
library(tidyverse)
library(lubridate)

# Set Working Dir
setwd("@WD@")

PaaS_type <- "@PAAS_TYPE@"
quantile_values <- c(.25, .50, .75, .90)

# Load datas
if (file.exists("azure_metrics.csv")) { 
    azure_metrics <- read.csv("azure_metrics.csv", stringsAsFactors = FALSE) 
    AZ   <- azure_metrics %>% mutate(duree = as.numeric(as.duration(hms(str_sub(Timestamp,-8))) - as.duration(hms(str_sub(min(Timestamp),-8))))/60) %>% pivot_wider(id_cols = duree, names_from = Name, values_from = c("Maximum"))

}
if (file.exists("aws_metrics.csv"))   { 
    aws_metrics   <- read.csv("aws_metrics.csv", stringsAsFactors = FALSE)   
    AWS  <- aws_metrics %>% mutate(duree = as.numeric(as.duration(hms(str_sub(Timestamp,-8))) - as.duration(hms(str_sub(min(Timestamp),-8))))/60) %>% pivot_wider(id_cols = duree,  names_from = Name, values_from = c("Maximum"))
}

result        <- read.csv("result.csv", stringsAsFactors = FALSE)
db_info       <- read.csv("db_info.csv", stringsAsFactors = FALSE)

# Prepare Datas
TPCC <- result %>% mutate(duree = round(elapsed / 60000)) %>% filter(ttype == 'NEW_ORDER')   %>% count(duree) %>% rename(nopm=n)
TPM  <- result %>% mutate(duree = round(elapsed / 60000)) %>% filter(ttype != 'DELIVERY_BG') %>% count(duree) %>% rename(tpm=n)
TPCC_TPM <- left_join(TPCC,TPM)

paas_conf <- fromJSON("paas_conf.csv", flatten=TRUE)

#max_tpm <- TPCC_TPM$tpm[!is.na(TPCC_TPM$tpm)] %>% mean()
#max_nopm<- TPCC_TPM$nopm[!is.na(TPCC_TPM$nopm)] %>% mean()

# TPM
quantile_tpm <- TPCC_TPM$tpm[!is.na(TPCC_TPM$tpm)] %>% quantile(na.rm = TRUE,quantile_values)
df_tpm <- data.frame(transpose(as.list(quantile_tpm)), stringsAsFactors = FALSE)
colnames(df_tpm) <- c("tpm_25","tpm_50","tpm_75","tpm_90")

# NOPM
quantile_nopm <- TPCC_TPM$nopm[!is.na(TPCC_TPM$nopm)] %>% quantile(na.rm = TRUE,quantile_values)
df_nopm <- data.frame(transpose(as.list(quantile_nopm)), stringsAsFactors = FALSE)
colnames(df_nopm) <- c("nopm_25","nopm_50","nopm_75","nopm_90")

# AWS / AZURE
if (exists('AZ') && is.data.frame(get('AZ'))){
    # IO
    if("IOPS" %in% colnames(AZ))
    {   
        quantile_IO <-  AZ$`IOPS`[!is.na(AZ$`IOPS`)]  %>% quantile(na.rm = TRUE,quantile_values)
    }else{
        quantile_IO <-  AZ$`IO percent`[!is.na(AZ$`IO percent`)] %>% quantile(na.rm = TRUE,quantile_values)
    }
    df_io <- data.frame(transpose(as.list(quantile_IO)), stringsAsFactors = FALSE)
    colnames(df_io) <- c("IO_25","IO_50","IO_75","IO_90")

    # CPU
    quantile_CPU <- AZ$`CPU percent`[!is.na(AZ$`CPU percent`)] %>% quantile(na.rm = TRUE,quantile_values)
    df_cpu <- data.frame(transpose(as.list(quantile_CPU)), stringsAsFactors = FALSE)
    colnames(df_cpu) <- c("CPU_25","CPU_50","CPU_75","CPU_90")

    # MEM
    quantile_MEM <- AZ$`Memory percent`[!is.na(AZ$`Memory percent`)] %>% quantile(na.rm = TRUE,quantile_values)
    df_mem <- data.frame(transpose(as.list(quantile_MEM)), stringsAsFactors = FALSE)
    colnames(df_mem) <- c("MEM_25","MEM_50","MEM_75","MEM_90")


    storage_GB <- paas_conf$properties$storageProfile$storageMB / 1024
    # 200000
    PaaS_size <- paas_conf$sku$name
    #"GP_Gen5_4"
    #df <- data.frame(PaaS_type,PaaS_size,storage_GB,max_tpm,max_nopm,max_CPU,max_IO,max_MEM, stringsAsFactors = FALSE)
    df <- data.frame(PaaS_type,PaaS_size,storage_GB,df_tpm,df_nopm,df_cpu,df_io,df_mem, stringsAsFactors = FALSE)
}else{
    
    AWS_PAAS_SIZES <- read.csv("../../AWS_PAAS_SIZE.csv", stringsAsFactors = FALSE)
    AWS_INSTANCE_CLASS <- paas_conf$DBInstances$DBInstanceClass
    AWS_ALLOCATED_MEM  <-  filter(AWS_PAAS_SIZES , Instance.class == AWS_INSTANCE_CLASS)$MEM
    AWS_ALLOCATED_CPU  <-  filter(AWS_PAAS_SIZES , Instance.class == AWS_INSTANCE_CLASS)$CPU
    AWS_ALLOCATED_CLASS  <-  filter(AWS_PAAS_SIZES , Instance.class == AWS_INSTANCE_CLASS)$CLASS
    
    #quantile_IO <-  AWS$`ReadIOPS`[!is.na(AWS$`ReadIOPS`)]  %>% quantile(na.rm = TRUE,quantile_values)
    quantile_IO <-  sum(AWS$`ReadIOPS`[!is.na(AWS$`ReadIOPS`)],AWS$`WriteIOPS`[!is.na(AWS$`WriteIOPS`)])  %>% quantile(na.rm = TRUE,quantile_values)
    df_io <- data.frame(transpose(as.list(quantile_IO)), stringsAsFactors = FALSE)
    colnames(df_io) <- c("IO_25","IO_50","IO_75","IO_90")


    # CPU
    quantile_CPU <- AWS$`CPUUtilization`[!is.na(AWS$`CPUUtilization`)] %>% quantile(na.rm = TRUE,quantile_values)
    df_cpu <- data.frame(transpose(as.list(quantile_CPU)), stringsAsFactors = FALSE)
    colnames(df_cpu) <- c("CPU_25","CPU_50","CPU_75","CPU_90")

    # MEM (AWS return Free mem so we need to make computations)
    #quantile_MEM <- AZ$`Memory percent`[!is.na(AZ$`Memory percent`)] %>% quantile(na.rm = TRUE)
    quantile_MEM <- ((100 * (AWS_ALLOCATED_MEM - (AWS$FreeableMemory/1024/1024/1024/1024))) / AWS_ALLOCATED_MEM) %>% quantile(na.rm = TRUE,quantile_values)
    df_mem <- data.frame(transpose(as.list(quantile_MEM)), stringsAsFactors = FALSE)
    colnames(df_mem) <- c("MEM_25","MEM_50","MEM_75","MEM_90")

    if (paas_conf$DBInstances$StorageType == "aurora") {
        storage_GB <- "1024"
    } else {
        storage_GB <- paas_conf$DBInstances$AllocatedStorage
    }    
    # 200000
    # Mise en forme comme sur Azure : GP ou MO CPUCount_
    PaaS_size <- paste(AWS_ALLOCATED_CLASS,AWS_ALLOCATED_CPU,AWS_ALLOCATED_MEM,sep="_")
    #"GP_Gen5_4"
    #df <- data.frame(PaaS_type,PaaS_size,storage_GB,max_tpm,max_nopm,max_CPU,max_IO,max_MEM, stringsAsFactors = FALSE)
    df <- data.frame(PaaS_type,PaaS_size,storage_GB,df_tpm,df_nopm,df_cpu,df_io,df_mem,AWS_INSTANCE_CLASS, stringsAsFactors = FALSE)    
}

write.csv(df,"maximuns.csv" ,row.names = TRUE)