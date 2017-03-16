#!/bin/bash

redis_server=""
if [[ $# -eq 0 ]]; then
    redis_server="localhost"
else
    redis_server=$1
fi

global_result_filename=result_global_$(date +%Y-%m-%d:%H:%M:%S)
nr_repetitions=10
jar_path="/home/cristiprg/BDAPRO.GlobalStateML/bdapro-ws1617-spark-jobs/target/bdapro-ws1617-spark-jobs-1.0-SNAPSHOT.jar"
jar_executable="java -cp $jar_path de.tu_berlin.dima.bdapro.spark.global_state_api.StateManagerStressTest"

row_start=100
row_end=100
row_incr=1

col_start=1
col_end=1000
col_incr=100

# write header for the global result
echo "nrRows,nrCols,mean,stddev" > $global_result_filename

for nr_rows in $(seq $row_start $row_incr $row_end); do
    for nr_cols in $(seq $col_start $col_incr $col_end); do
        result_filename=result_LOAD_${nr_rows}_${nr_cols}.csv
        test_cmd="$jar_executable $redis_server $nr_repetitions $nr_rows $nr_cols $result_filename LOAD"

        # run the test with the given params and write the results in the global file
        echo "nr_rows=${nr_rows}, nr_cols=${nr_cols}"
        test_result=$(eval $test_cmd)
        echo $test_result >> $global_result_filename

    done
done

