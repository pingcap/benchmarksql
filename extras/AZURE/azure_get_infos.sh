#!/bin/bash
# ----
# Script to extract metrics from azure PostgreSQL PaaS (simple and flexible servers)
#
# Copyright (C) 2020, Nicolas MARTIN @Capdata
#
# ----
# This script is intead to extract metrics from Azure PostgreSQL PaaS for reporting purpose
# Print reults on screen and save it on $MY_RESULT/paas_conf.csv
#

function usage(){
    echo "Usage : "
    echo " need 4 input parameters : "
    echo "    - PaaS name"
    echo "    - Ressource group name"
    echo "    - PaaS type must be simple or flexible"
    echo "    - direcory of benchmarksql run (containing informations about date and duration)"
    exit 0
}

if [ $# -eq 3 ] ; then
	PAAS=$1
    RG=$2
	TYP=$3
	MY_RESULT="$4/data"
else
    usage
fi	

case $TYP in 
"simple")
	TYPE="servers"
    IO="io_consumption_percent" 
;;
"flexible")
	IO="iops"
	TYPE="flexibleServers"
;;
*)
    usage
;;

[ ! -f ${MY_RESULT}/runInfo.csv ] && echo "${MY_RESULT} may contains runInfo.csv !!!" && exit 1

date_deb=$(cat ${MY_RESULT}/runInfo.csv | tail -1 | cut -d',' -f5 | sed -e 's/\..*//g')
duration=$(cat ${MY_RESULT}/runInfo.csv | tail -1 | cut -d',' -f6)
date_fin=$(date +'%Y-%m-%d %T' --date="$date_deb $duration minutes")
echo "$date_deb => $date_fin"
az monitor metrics list --resource $PAAS --resource-group $RG --resource-type Microsoft.DBforPostgreSQL/$TYPE --metrics "cpu_percent" "memory_percent" $IO "active_connections" --aggregation maximum average --start-time $(echo $date_deb | sed -e 's/ /T/') --end-time $(echo $date_fin | sed -e 's/ /T/') -o table | sed -e 's/\s\{2,\}/,/g' | grep -v "\----" |tee  $MY_RESULT/azure_metrics.csv
az resource show -n $PAAS -g $RG --resource-type Microsoft.DBforPostgreSQL/$TYPE | tee $MY_RESULT/paas_conf.csv

