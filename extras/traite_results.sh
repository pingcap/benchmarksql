#!/bin/bash
# ----
# Script to extract details from BenchmarkSQL run.
#
# Copyright (C) 2020, Nicolas MARTIN @Capdata
#
# ----
# This script is intead to parse results from benchmaksql capture metrics (and / or Clouder Metrics)
# and made sumup from it (extract of percentile and configuration values), see graphs.R and extract_max_avg.R 
#
# All directories corresponding to a specific mask (default is "my_result_", as it's the default mask configuration for directory) 
# are parsed for graphing and extracting percentiles values. 
# Note : You can also set MAX_ONLY to 1 (ie : export MAX_ONLY=1) before running it to just extract percentiles values 
#
# Parameters :
# 
# - PAAS_TYPE : a string identifier for final report 
# - FILTRE    : a string to limit directories to work with, default value is : my_result_
# - FORCE     : -f : to force rebuilding all things ()    , default is to skip when resulting files are found
# 
# Prerequisites : 
#  Need to install R with following packages : 
#                        jsonlite
#                        tidyverse
#                        lubridate
#                        ggplot2
#                        hrbrthemes
#                        viridis
#                        htmlwidgets
#
# ------------
#
#
#

function usage(){
    echo "This script is intead to parse results from benchmaksql capture metrics (and / or Clouder Metrics)"
    echo "and made sumup from it (extract of percentile and configuration values), see graphs.R and extract_max_avg.R"
    echo ""
    echo "All directories corresponding to a specific mask (default is 'my_result_', as it's the default mask configuration for directory) "
    echo "are parsed for graphing and extracting percentiles values. "
    echo "Note : You can also set MAX_ONLY to 1 (ie : export MAX_ONLY=1) before running it to just extract percentiles values "
    echo ""
    echo "Parameters :"
    echo ""
    echo "- PAAS_TYPE : a string identifier for final report "
    echo "- FILTRE    : a string to limit directories to work with, default value is : my_result_"
    echo "- FORCE     : -f : to force rebuilding all things ()    , default is to skip when resulting files are found"
}

DEBUG=0
[ -z $MAX_ONLY ] && MAX_ONLY=0
FORCE=0

case $# in
3)
    FORCE=$3 
    FILTRE=$2
    PaaS_TYPE=$1
;;
2)
    FILTRE=$2
    PaaS_TYPE=$1
;;
1)
    PaaS_TYPE=$1
   	FILTRE="my_result_" 
;;
*)
    echo "Must provide at least Paas Type"
    usage
    exit 99
;;
esac

if [ "$FORCE" == "-f" ];then
    FORCE=1
else
    FORCE=0
fi


WIDTH=540
HEIGHT=180


# Extract result, create graphs and extract max
for d in $(ls  | grep "$FILTRE"); do 
    
    if [ "$FILTRE" == "2020" ] ; then
	    PaaS_TYPE=$(echo $d | sed -e 's/_2020.*//g')
    fi
    datadir=$(echo $d/data)

    ARRAY=($(cat $datadir/paas_conf.csv | egrep "storageMB|family|capacity|name"| sed -e 's/"//g' -e 's/ *//g' -e 's/,//g'))
    STORAGE=$(echo ${ARRAY[1]} | cut -d':' -f2)
    STORAGE=$(echo $STORAGE / 1024 | bc)
    CAPACITY=$(echo ${ARRAY[2]} | cut -d':' -f2)
    TYPE=$(echo ${ARRAY[4]} | cut -d':' -f2)

    TITLE="${TYPE}-${CAPACITY}-${STORAGE}"

    echo "Generate report for : $d"
    echo $TITLE
    
    if [[ ! -f $datadir/p_res.png  ||  $FORCE -eq 1 || ! -f  $datadir/p_res.png ]] && [[ $MAX_ONLY -eq 0 ]] ; then
        [ $MAX_ONLY -eq 0 ] && out=$(sed -e "s/@WIDTH@/${WIDTH}/g" -e "s/@HEIGHT@/${HEIGHT}/g" -e "s/@TITLE@/${TITLE}/g" -e "s|@WD@|${datadir}|g" -e "s|@PAAS_TYPE@|${PaaS_TYPE}|g" < ./graphs.R | R --no-save 2>/dev/null 2>&1)
    fi
    if [[ ! -f $datadir/maximuns.csv  ||  $FORCE -eq 1 ]] ; then
        echo "----------------------------------------------------------"
        echo "Extracting interresting values"
        rm $datadir/maximuns.csv
        cat extract_max_avg.R  | sed -e "s/@WIDTH@/${WIDTH}/g" -e "s/@HEIGHT@/${HEIGHT}/g" -e "s/@TITLE@/${TITLE}/g" -e "s|@WD@|${datadir}|g" -e "s|@PAAS_TYPE@|${PaaS_TYPE}|g" > debug_R.r
        out=$(sed -e "s/@WIDTH@/${WIDTH}/g" -e "s/@HEIGHT@/${HEIGHT}/g" -e "s/@TITLE@/${TITLE}/g" -e "s|@WD@|${datadir}|g" -e "s|@PAAS_TYPE@|${PaaS_TYPE}|g" < ./extract_max_avg.R | R --no-save 2>/dev/null 2>&1)
	    echo "--------------------------------------------------------------------------------------------------------------------"
    fi
done

# Compulse MAX
rm ${PaaS_TYPE}_max_all.csv
ENTETE=0
for d in $(ls  | grep "$FILTRE"); do 
    datadir=$(echo $d/data)
    if [ "$FILTRE" == "2020" ] ; then
            PaaS_TYPE=$(echo $d | sed -e 's/_2020.*//g')
    fi
    if [ $ENTETE -eq 0 ] ; then
        cat $datadir/maximuns.csv > ${PaaS_TYPE}_max_all.csv
        [ $? -eq 0 ] && ENTETE=1
    else
        tail -1 $datadir/maximuns.csv >> ${PaaS_TYPE}_max_all.csv
    fi
done
