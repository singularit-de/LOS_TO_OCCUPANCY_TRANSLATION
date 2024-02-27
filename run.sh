#!/bin/bash
declare -a normal=("los_error_diff.csv" "occ.csv" "occupied_error_diff_new.csv" "occupied_error_diff_old.csv" "occupied_error_within_year_new.csv")
declare -a narrow=("narrow_los_error_diff.csv" "narrow_occ.csv" "narrow_occupied_error_diff_new.csv" "narrow_occupied_error_diff_old.csv" "narrow_occupied_error_within_year_new.csv")
declare -a symmetric=("symmetric_los_error_diff.csv" "symmetric_occ.csv" "symmetric_occupied_error_diff_new.csv" "symmetric_occupied_error_diff_old.csv" "symmetric_occupied_error_within_year_new.csv")
declare -a symmetric_narrow=("symmetric_narrow_los_error_diff.csv" "symmetric_narrow_occ.csv" "symmetric_narrow_occupied_error_diff_new.csv" "symmetric_narrow_occupied_error_diff_old.csv" "symmetric_narrow_occupied_error_within_year_new.csv")
declare -a extra=("bed_occupancy.csv" "occupancy.csv")

psql -c "CREATE DATABASE healthinf"

psql healthinf < admissions.sql
psql healthinf < evaluation.sql

psql healthinf < normal.sql
for file in "${normal[@]}"
do
    cp '/tmp/'$file plot/normal/
done
cd plot/normal; gnuplot plot.txt; cd ../..

psql healthinf < narrow.sql
for file in "${narrow[@]}"
do
    cp '/tmp/'$file plot/narrow/
done
cd plot/narrow; gnuplot plot.txt; cd ../..

psql healthinf < symmetric.sql
for file in "${symmetric[@]}"
do
    cp '/tmp/'$file plot/symmetric/
done
cd plot/symmetric; gnuplot plot.txt; cd ../..

psql healthinf < symmetric_narrow.sql
for file in "${symmetric_narrow[@]}"
do
    cp '/tmp/'$file plot/symmetric_narrow/
done
cd plot/symmetric_narrow; gnuplot plot.txt; cd ../..

psql healthinf < extra.sql
for file in "${extra[@]}"
do
    cp '/tmp/'$file plot/extra/
done
cd plot/extra; gnuplot plot.txt; cd ../..
