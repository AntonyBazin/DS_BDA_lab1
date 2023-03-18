#!/bin/bash
if [[ $# -lt 3 ]] ; then
    echo 'Usage: ./generateInputData.sh output_file_name id_limit time_scale'
    exit 1
fi

if [[ $# -gt 3 ]] ; then
    echo 'Too many arguments!'
    exit 1
fi

rm -rf input
mkdir input

MAX_DELAY=$3

LOG_ID_LIMIT=$2

# A procedure to write some lines
makeLogLine() {
  sleep $((RANDOM % MAX_DELAY))
  DATE=$(/usr/bin/date "+%s")
  RESULT="$((RANDOM % LOG_ID_LIMIT)), $DATE, $(shuf -i 10-99 -n 1)"
  echo "$RESULT" >> "input/$1.txt"
}

for _ in {1..200}
  do
    makeLogLine "$@"
  done

