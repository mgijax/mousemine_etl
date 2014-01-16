#!/bin/sh
python ./dumpMgiItemXml.py -d ../output/mgi-base -Demapxfile=../output/obo/EMAPX.obo -Dmedicfile=../output/obo/MEDIC_conflated.obo --logfile ./LOG
e=$?; if [ $e -ne 0 ]; then exit $e; fi

sed -i".old" '/value="Not Applicable"/d' ../output/mgi-base/*.xml
e=$?; if [ $e -ne 0 ]; then exit $e; fi
rm ../output/mgi-base/*.old
