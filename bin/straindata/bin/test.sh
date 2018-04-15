#!/bin/bash
while read -u 10 p; do
    echo ""
    IFS='	' read -r -a array <<< "${p}"
    mgiid="${array[0]}"
    strain="${array[1]}"
    url="${array[2]}"
    echo "${mgiid}" "${strain}" "${url}"
done 10<config2.cfg
