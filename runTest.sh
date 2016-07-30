#!/usr/bin/env bash

FILENAME="/home/Francine/Dataset/Thyroid/ann-test.data"
INPUTDATA="/home/Francine/Dataset/Thyroid/ann-train.data"
rm -r record.txt recordR.txt recordS.txt recordSusy.txt
#shuf retrieves a random line, awk cuts out the last column, sed changes the whitespaces for commas
#shuf -n 1 < ${FILENAME} >> recordR.txt
#cat recordR.txt | awk 'NF{NF--};1'| sed -e "s/ /,/g" >> record.txt
#RECORD=`cat record.txt`
#flink run ./target/flink_knn-0.1.jar --k 10 --m gower --s ${RECORD} --in ${INPUTDATA} --outputPath /home/Francine/flink_knn.csv --set Thyroid
shuf -n 1 < /home/Francine/SUSY/SUSY_1K.csv >> recordS.txt
cat recordS.txt | awk 'BEGIN{FS=OFS=","}{$1="";sub(",","")}1' >> recordSusy.txt
RECORDSUSY=`cat recordSusy.txt`
INPUTS="/home/Francine/SUSY/SUSY_90.csv"
time flink run ./target/flink_knn-0.1.jar --k 11 --m euclidean --s ${RECORDSUSY} --in ${INPUTS} --outputPath /home/Francine/flink_knn_susy.csv --set susy
#flink run ./target/flink_knn-0.1.jar --k 10 --m gower --s ${RECORDSUSY} --in ${INPUTS} --outputPath /home/Francine/flink_knn_susy1.csv --set susy