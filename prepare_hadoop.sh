# Remove old data from the DFS
hdfs dfs -rm -r input output

# Send new data to DFS
hdfs dfs -put input input
