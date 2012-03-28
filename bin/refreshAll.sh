#!/bin/sh

python ./refreshSynteny.py > ../output/ncbi-synblock/SyntenicRegion.xml
./refreshObo.sh
./refreshItems.sh
