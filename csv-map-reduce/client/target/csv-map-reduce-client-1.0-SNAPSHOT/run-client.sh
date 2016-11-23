#!/bin/bash

java -cp 'lib/jars/*' -Dquery=3 -DinPath="/Users/melisaanabellarossi/Documents/workspace/prueba/csv-map-reduce/client/src/main/resources/files/dataset-1000.csv"  -Dname='54080-54265'  -Dpass=dev-pass  -Daddresses='192.168.1.120'   "csv.map.reduce.client.CensusClient" $*

