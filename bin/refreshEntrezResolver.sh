#!/bin/sh

# BASE
BASEDIR=..

# Main output directory
ODIR=$BASEDIR/output/entrez
if [ ! -d $ODIR ]
then
    mkdir -p $ODIR
fi


# MEDIC
curl ftp://ftp.ncbi.nih.gov/gene/DATA/gene_info.gz | zcat > $ODIR/gene_info
if [ $? -ne 0 ] 
then
    exit $? 
fi

