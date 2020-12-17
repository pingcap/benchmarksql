!/bin/bash
# ----
# Script to extract metrics from AWS PostgreSQL PaaS (Aurora or Classical RDS, it may also work for RDS other than PostgreSQL)
#
# Copyright (C) 2020, Nicolas MARTIN @Capdata
#
# ----
# This script is intead to extract metrics from AWS RDS PaaS for reporting purpose
# Get metrics, format in csv ,print reults on screen and save it on $MY_RESULT/aws_metrics.csv
#
# Also save PaaS configuration (name, intance type storage size, etc) as $MY_RESULT/paas_conf.csv
#

function usage(){
    echo "Usage : "
    echo " need 4 input parameters : "
    echo "    - PaaS name"
    echo "    - Region"
    echo "    - PaaS type must be simple or flexible"
    echo "    - direcory of benchmarksql run (containing informations about date and duration)"
    exit 0
}

if [ $# -eq 3 ] ; then
	PAAS=$1
    REGION=$2
	TYP=$3
	MY_RESULT="$4/data"
else
    usage
fi	

if [ $# -ge 2  ] ; then
	PAAS=$1
        [ $2 == "-ep" ] && aws rds describe-db-instances  --region $REGION --db-instance-identifier $PAAS | grep "Address" | cut -d':' -f2 | sed -e 's/ "//g' -e 's/",//g' && exit 0
	MY_RESULT="$2/data"
else
        PAAS=$1
        MY_RESULT="$(ls -ltrd */ | tail -1 | awk '{print $9}')/data"
fi	


[ -z $PAAS ] && echo "PaaS is null please provide parameter" &&  exit 99

date_deb=$(cat ${MY_RESULT}/runInfo.csv | tail -1 | cut -d',' -f5 | sed -e 's/\..*//g')
duration=$(cat ${MY_RESULT}/runInfo.csv | tail -1 | cut -d',' -f6)
date_fin=$(date +'%Y-%m-%d %T' --date="$date_deb $duration minutes")
echo "$date_deb => $date_fin"
#az monitor metrics list --resource $PAAS --resource-group NMN_TestPG --resource-type Microsoft.DBforPostgreSQL/$TYPE --metrics "cpu_percent" "memory_percent" $IO "active_connections" --aggregation maximum average --start-time $(echo $date_deb | sed -e 's/ /T/') --end-time $(echo $date_fin | sed -e 's/ /T/') -o table | sed -e 's/\s\{2,\}/,/g' | grep -v "\----" |tee  $MY_RESULT/azure_metrics.csv
#az resource show -n $PAAS -g NMN_TestPG --resource-type Microsoft.DBforPostgreSQL/$TYPE | tee $MY_RESULT/paas_conf.csv

cat template_metric_query.json | sed -e "s/@PAAS@/$PAAS/g" > metric_query.json

aws cloudwatch get-metric-data  --start-time "$(echo $date_deb | sed -e 's/ /T/')" --end-time "$(echo $date_fin | sed -e 's/ /T/')" --region $REGION --metric-data-queries file://./metric_query.json > outmetric.json

bash process_metricjson.sh | tee $MY_RESULT/aws_metrics.csv

aws rds describe-db-instances  --region $REGION --db-instance-identifier $PAAS > $MY_RESULT/paas_conf.csv
