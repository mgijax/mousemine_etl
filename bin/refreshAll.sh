#!/bin/sh

NCBI_DIR=../output/ncbi-synblock
if [ ! -d $NCBI_DIR ]
then
    mkdir -p $NCBI_DIR
fi

##
python ./refreshSynteny.py > ../output/ncbi-synblock/SyntenicRegion.xml
python ./idChecker.py ../output/ncbi-synblock/SyntenicRegion.xml
if [ $? -ne 0 ] 
then
    exit -1 
fi

##
./refreshObo.sh
if [ $? -ne 0 ] 
then
    exit -1 
fi

##
./refreshItems.sh
if [ $? -ne 0 ] 
then
    exit -1 
fi
