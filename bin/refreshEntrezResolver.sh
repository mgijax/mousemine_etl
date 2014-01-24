#!/bin/sh

# BASE
BASEDIR=..

# Main output directory
ODIR=$BASEDIR/output/idresolver
if [ ! -d $ODIR ]
then
    mkdir -p $ODIR
fi


curl ftp://ftp.ncbi.nih.gov/gene/DATA/gene_info.gz | zcat > $ODIR/entrez
e=$?; if [ $e -ne 0 ]; then exit $e; fi

