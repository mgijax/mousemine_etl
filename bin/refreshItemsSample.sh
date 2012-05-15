#!/bin/sh
python ./dumpMgiItemXml.py -d ../output/mgi-sample -Dmoshfile=../output/obo/MOSH.obo --logfile ./LOG --install Sampler
if [ $? -ne 0 ] 
then
    exit -1 
fi
python ./idChecker.py ../output/mgi-sample
if [ $? -ne 0 ] 
then
    exit -1 
fi
