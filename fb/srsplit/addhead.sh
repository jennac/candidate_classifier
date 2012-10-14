#!/bin/bash
list=`ls -1 fullfbsearch_results_combined*`
header=`head -n 1 fullfbsrc.csv`
for x in $list
do
	echo $header|cat - $x > /tmp/out && mv /tmp/out $x
done
