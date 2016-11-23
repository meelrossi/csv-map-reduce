#!/bin/bash

java -cp 'lib/jars/*'  -Dname='54080-54265'  -Dpass=dev-pass  -Daddresses='127.0.0.1'   "cvs.map.reduce.client.CensusClient" $*

