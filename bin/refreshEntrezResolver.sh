#!/bin/sh

# BASE
BASEDIR=..

# Main output directory
ODIR=$BASEDIR/output/idresolver
if [ ! -d $ODIR ]
then
    mkdir -p $ODIR
fi


# MEDIC
curl ftp://ftp.ncbi.nih.gov/gene/DATA/gene_info.gz | zcat > $ODIR/entrez
if [ $? -ne 0 ] 
then
    exit $? 
fi

