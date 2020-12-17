#!/bin/bash
# ----
# Script to transform json AWS metrics to csv
#
# Copyright (C) 2020, Nicolas MARTIN @Capdata
#
# ----
#
JQ=./jq-linux64
if [ $# -eq 0 ] ; then
	METRIC=outmetric.json
else
	METRIC=$1
fi
DEBUG=0
METRIC_NAME=""
ENTETE="Timestamp,Name,Average,Maximum"

#Timestamp	Name	Average	Maximum

OLDIFS=$IFS
IFS=$'\n'

METRICS=(`cat $METRIC | $JQ '.MetricDataResults[] | .Label'`)
TIMESTAMP=(`cat $METRIC | $JQ '.MetricDataResults[0] | .Timestamps[]'`)

COUNT=${#METRICS[@]}
echo $ENTETE

for i in ${!METRICS[@]}; do
    METRIC_NAME=$(echo ${METRICS[$i]} | cut -d' ' -f1 | sed -e 's/"//g')
    METRIC_TYPE=$(echo ${METRICS[$i]} | cut -d' ' -f2 | sed -e 's/"//g')
    [ $i -eq 0 ] && METRIC_NAME_OLD=$METRIC_NAME
    if [ "$METRIC_NAME" != "$METRIC_NAME_OLD" -a $i -ne $(echo "$COUNT-1" | bc) ] ; then
        [ $DEBUG -eq 1 ] && echo " $METRIC_NAME != $METRIC_NAME_OLD"
        for j in ${!TIMESTAMP[@]}; do
            echo "${TIMESTAMP[$j]},$METRIC_NAME_OLD,${ARRAY_AVERAGE[$j]},${ARRAY_MAXIMUM[$j]}"
        done
    fi
    METRIC_NAME_OLD=$METRIC_NAME

    case "$METRIC_TYPE" in
    "Average") 
	    [ $DEBUG -eq 1 ] && echo "Average array for metric $METRIC_NAME"
	    ARRAY_AVERAGE=(`cat $METRIC | $JQ ".MetricDataResults[$i] | .Values[]"`)
	    ;;
    "Maximum") 
	    [ $DEBUG -eq 1 ] && echo "Maximum array for metric $METRIC_NAME"
	    ARRAY_MAXIMUM=(`cat $METRIC | $JQ ".MetricDataResults[$i] | .Values[]"`)
	    ;;
    *) [ $DEBUG -eq 1 ] && echo $METRIC_TYPE;exit 99;;
    esac
    #exit 99
done

[ $DEBUG -eq 1 ] && echo " $METRIC_NAME != $METRIC_NAME_OLD"
for j in ${!TIMESTAMP[@]}; do
    echo "${TIMESTAMP[$j]},$METRIC_NAME_OLD,${ARRAY_AVERAGE[$j]},${ARRAY_MAXIMUM[$j]}"
done

IFS=$OLDIFS
