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

# MOSH
curl http://ctdbase.org/reports/CTD_diseases.obo.gz | zcat > $XDIR/CTDMOSH.obo
python ReMOSH.py $XDIR/CTDMOSH.obo $XDIR/spreadsheet.tsv > $ODIR/MOSH.obo

# GO
curl -o $ODIR/GeneOntology.obo http://www.geneontology.org/ontology/obo_format_1_0/gene_ontology.1_0.obo

# MP
curl -o $ODIR/MammalianPhenotype.obo ftp://ftp.informatics.jax.org/pub/reports/MPheno_OBO.ontology

# Adult Mouse Anatomy
#curl -o $ODIR/AdultMouseAnatomy.obo ftp://ftp.informatics.jax.org/pub/reports/adult_mouse_anatomy.obo

# ChEBI - Chemical Entities of Biological Interest
#curl -o $ODIR/ChEBI.obo ftp://ftp.ebi.ac.uk/pub/databases/chebi/ontology/chebi.obo

