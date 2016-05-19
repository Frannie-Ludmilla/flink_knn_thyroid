#!/usr/bin/env bash

FILENAME="/home/Francine/Dataset/Thyroid/ann-test.data"
INPUTDATA="/home/Francine/Dataset/Thyroid/ann-train.data"
rm -r record.txt
#rm -r recordR.txt
#shuf retrieves a random line, awk cuts out the last column, sed changes the whitespaces for commas
#shuf -n 1 < ${FILENAME} >> recordR.txt
cat recordR.txt | awk 'NF{NF--};1'| sed -e "s/ /,/g" >> record.txt
RECORD=`cat record.txt`
flink run ./target/flink_knn-0.1.jar --k 10 --m gower --s ${RECORD} --in ${INPUTDATA} --outputPath /home/Francine/flink_knn.csv --set Thyroid