# Remove old data from the local FS and DFS
rm -r output/*
hdfs dfs -rm -r input output

# Send new data to DFS
hdfs dfs -put input input
yarn jar target/lab1-1.0-SNAPSHOT-jar-with-dependencies.jar input output 60
hdfs dfs -get output output
