#!/bin/bash

java -cp 'lib/jars/*'  -Dname='54080-54265'  -Dpass=dev-pass -Dquery=4 -Daddresses='127.0.0.1'   "csv.map.reduce.client.CensusClient" $*

