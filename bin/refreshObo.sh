#!/bin/sh

# BASE
BASEDIR=..

# Main output directory
ODIR=$BASEDIR/output/obo
if [ ! -d $ODIR ]
then
    mkdir -p $ODIR
fi
# Temp/working directroy
TDIR=$BASEDIR/tmp
# Extras/resources directory
XDIR=$BASEDIR/resources


# MEDIC
curl https://gillnet.mdibl.org/~twiegers/mgi/mgiMEDIC.obo.gz | zcat > $ODIR/MEDIC.obo
if [ $? -ne 0 ] 
then
    exit $? 
fi


# GO
curl -o $ODIR/GeneOntology.obo http://www.geneontology.org/ontology/obo_format_1_0/gene_ontology.1_0.obo
if [ $? -ne 0 ] 
then
    exit $?
fi


# MP
curl -o $ODIR/MammalianPhenotype.obo ftp://ftp.informatics.jax.org/pub/reports/MPheno_OBO.ontology
if [ $? -ne 0 ] 
then
    exit $? 
fi


# Adult Mouse Anatomy
#curl -o $ODIR/AdultMouseAnatomy.obo ftp://ftp.informatics.jax.org/pub/reports/adult_mouse_anatomy.obo

# ChEBI - Chemical Entities of Biological Interest
#curl -o $ODIR/ChEBI.obo ftp://ftp.ebi.ac.uk/pub/databases/chebi/ontology/chebi.obo

