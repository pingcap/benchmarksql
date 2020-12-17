# Libraries
# Need following sys packages : sudo apt install r-cran-curl r-cran-tidyverse r-cran-plotly r-cran-ggplot2 r-cran-lubridate r-cran-viridis

list.of.packages <- c("hrbrthemes")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)
library(tidyverse)
library(lubridate)
library(ggplot2)
library(hrbrthemes)
library(plotly)
library(viridis)
library(htmlwidgets)
library(jsonlite)

hrbrthemes::import_roboto_condensed()

# Set Working Dir
setwd("@WD@")



# Load datas
azure_metrics <- read.csv("azure_metrics.csv", stringsAsFactors = FALSE)
result        <- read.csv("result.csv", stringsAsFactors = FALSE)
db_info       <- read.csv("db_info.csv", stringsAsFactors = FALSE)
paas_conf     <- fromJSON("paas_conf.csv", flatten=TRUE)

# Prepare Datas
TPCC <- result %>% mutate(duree = round(elapsed / 60000)) %>% filter(ttype == 'NEW_ORDER')   %>% count(duree) %>% rename(nopm=n)
TPM  <- result %>% mutate(duree = round(elapsed / 60000)) %>% filter(ttype != 'DELIVERY_BG') %>% count(duree) %>% rename(tpm=n)
TPCC_TPM <- left_join(TPCC,TPM)

AZ   <- azure_metrics %>% mutate(duree = as.numeric(as.duration(hms(str_sub(Timestamp,-8))) - as.duration(hms(str_sub(min(Timestamp),-8))))/60) %>% pivot_wider(id_cols = duree, names_from = Name, values_from = c("Maximum"))
#DB   <- db_info %>% filter(wait_event_type != '') %>% mutate(duree = round(time / 60000)) %>% pivot_wider(id_cols = duree, names_from = wait_event_type, values_from = count,values_fn = sum)

# Graphs
coeff  <- max(TPM$tpm)/100
colors <- c( "nopm" = "orange", "tpm" = "red" ,"%IO" = "blue", "%CPU" = "yellow", "%MEM" = "green")
if("IOPS" %in% colnames(AZ))
{
    ##AZ MAX IOPS / Compute type
    #D2s_v3	3200
    #D4s_v3	6400 
    #D8s_v3	12800
    #D16s_v3	18000
    #D32s_v3	18000
    #D48s_v3	18000
    #D64s_v3	18000
    #E2s_v3	3200
    #E4s_v3	6400 
    #E8s_v3	12800
    #E16s_v3	18000
    #E32s_v3	18000
    #E48s_v3	18000
    #E64s_v3	18000
    storage_GB <- paas_conf$properties$storageProfile$storageMB / 1024
        
    az_fs_storage <- c(32,64,128,256,512,1024,2048,4096,8192,16384)
    az_fs_maxiops <- c(120,240,500,1100,2300,5000,7500,7500,16000,18000)
    az_iops.data <- data.frame(az_fs_storage,az_fs_maxiops)
    
    #Compute coeffIOPS (needed for conversion IOPS => %IO)
    MAX_IOPS=(filter (az_iops.data,az_fs_storage == storage_GB))
    coeffIOPS <- 100 / MAX_IOPS$az_fs_maxiops

    p_res <- left_join(AZ,TPCC_TPM) %>%
        ggplot    (aes(x=duree)) +  
        geom_line (aes(y = nopm                         ,color = "nopm"))  +
        geom_point(aes(y = nopm                         ,color = "nopm"))  +
        geom_line (aes(y = tpm                          ,color = "tpm"))   +
        geom_point(aes(y = tpm                          ,color = "tpm"))   +    
        geom_line (aes(y = `IOPS` * coeffIOPS* coeff    ,color = "%IO"  )  ,linetype = "dashed") +
        geom_line (aes(y = `CPU percent`     * coeff    ,color = "%CPU" )  ,linetype = "dashed") +
        geom_line (aes(y = `Memory percent`  * coeff    ,color = "%MEM" )  ,linetype = "dashed") +
        labs(color = "") +
        scale_color_manual(values = colors) +
        scale_y_continuous(
            name = "Transactions per Minute",
            sec.axis = sec_axis(~./ coeff ,name="% Res usage")
        ) +
        ggtitle("@TITLE@") +
        xlab("Elapsed minutes") +
        theme_ipsum() +
        xlab("")    
}else{
    p_res <- left_join(AZ,TPCC_TPM) %>%
        ggplot    (aes(x=duree)) +  
        geom_line (aes(y = nopm                       ,color = "nopm"))  +
        geom_point(aes(y = nopm                       ,color = "nopm"))  +
        geom_line (aes(y = tpm                        ,color = "tpm"))   +
        geom_point(aes(y = tpm                        ,color = "tpm"))   +    
        geom_line (aes(y = `IO percent`     * coeff   ,color = "%IO"  )  ,linetype = "dashed") +
        geom_line (aes(y = `CPU percent`    * coeff   ,color = "%CPU" )  ,linetype = "dashed") +
        geom_line (aes(y = `Memory percent` * coeff   ,color = "%MEM" )  ,linetype = "dashed") +
        labs(color = "") +
        scale_color_manual(values = colors) +
        scale_y_continuous(
            name = "Transactions per Minute",
            sec.axis = sec_axis(~./ coeff ,name="% Res usage")
        ) +
        ggtitle("@TITLE@") +
        xlab("Elapsed minutes") +
        theme_ipsum() +
        xlab("")
}
#p_res  


# Wait event type (bar chart)
p_db <- db_info %>% filter(wait_event_type != '') %>% mutate(duree = round(time / 60000)) %>%
ggplot(aes(fill=wait_event_type , y=count, x=duree)) + 
    geom_bar(position="stack", stat="identity") +
    scale_fill_viridis(discrete = T) +
    ggtitle("Database wait events / min") +
    theme_ipsum() +
    xlab("Elapsed minutes")

# Save ggplot
ggsave("p_res.png", plot = p_res, width=@WIDTH@, height=@HEIGHT@ ,units = "mm")
ggsave("p_db.png", plot =  p_db,  width=@WIDTH@, height=@HEIGHT@ ,units = "mm")