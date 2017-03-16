#!/bin/bash

redis_server=""
if [[ $# -eq 0 ]]; then
    redis_server="localhost"
else
    redis_server=$1
fi

nr_repetitions=10
jar_executable="java -cp /home/prigoana/bdapro-ws1617-spark-jobs-1.0-SNAPSHOT.jar de.tu_berlin.dima.bdapro.spark.global_state_api.StateManagerStressTest"

row_start=1000
row_end=1001
row_incr=1

col_start=1
col_end=10000
col_incr=500

for nr_rows in $(seq $row_start $row_incr $row_end); do
    for nr_cols in $(seq $col_start $col_incr $col_end); do
        result_filename=result_LOAD_${nr_rows}_${nr_cols}.csv
        test_cmd="$jar_executable $redis_server $nr_repetitions $nr_rows $nr_cols $result_filename LOAD"
        echo "nr_rows=${nr_rows}, nr_cols=${nr_cols}"
        #eval $test_cmd
    done
done

