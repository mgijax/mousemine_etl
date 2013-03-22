#!/bin/sh
python ./dumpMgiItemXml.py -d ../output/mgi-base -Dmedicfile=../output/obo/MEDIC_conflated.obo --logfile ./LOG

sed -i".old" '/value="Not Applicable"/d' ../output/mgi-base/*.xml
rm ../output/mgi-base/*.old


if [ $? -ne 0 ] 
then
    exit -1 
fi
