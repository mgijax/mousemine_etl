#!/bin/sh

SYN_DIR=../output/mgi-synblock
if [ ! -d $SYN_DIR ]
then
    mkdir -p $SYN_DIR
fi

##
python ./refreshSynteny.py > ${SYN_DIR}/SyntenicRegion.xml
python ./idChecker.py ${SYN_DIR}/SyntenicRegion.xml
if [ $? -ne 0 ] 
then
    exit 1 
fi

##
./refreshObo.sh
if [ $? -ne 0 ] 
then
    exit 1 
fi

##
./refreshItems.sh
if [ $? -ne 0 ] 
then
    exit 1 
fi
