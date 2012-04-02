#!/bin/sh

NCBI_DIR=../output/ncbi-synblock
if [ ! -d $NCBI_DIR ]
then
    mkdir -p $NCBI_DIR
fi

python ./refreshSynteny.py > ../output/ncbi-synblock/SyntenicRegion.xml
./refreshObo.sh
./refreshItems.sh
