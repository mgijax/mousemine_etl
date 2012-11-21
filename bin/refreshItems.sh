#!/bin/sh
python ./dumpMgiItemXml.py -d ../output/mgi-base -Dmedicfile=../output/obo/MEDIC_plusMgiOmim.obo --logfile ./LOG
if [ $? -ne 0 ] 
then
    exit -1 
fi
