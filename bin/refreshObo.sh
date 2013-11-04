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
    exit -1 
fi

python $BASEDIR/bin/libdump/MedicConflater.py $ODIR/MEDIC.obo $ODIR/MEDIC_conflated.obo


# GO
curl -o $ODIR/GeneOntology.obo http://www.geneontology.org/ontology/obo_format_1_0/gene_ontology.1_0.obo
if [ $? -ne 0 ] 
then
    exit -1
fi


# MP
curl -o $ODIR/MammalianPhenotype.obo ftp://ftp.informatics.jax.org/pub/reports/MPheno_OBO.ontology
if [ $? -ne 0 ] 
then
    exit -1 
fi


# UBERON
curl -o $ODIR/uberon.obo http://obo.svn.sourceforge.net/svnroot/obo/uberon/trunk/uberon.obo
if [ $? -ne 0 ] 
then
    exit -1 
fi

# EMAP (Edinburgh Anatomy)
curl -o $ODIR/EMAP.obo ftp://ftp.hgu.mrc.ac.uk/pub/MouseAtlas/Anatomy/EMAP_combined.obo

if [ $? -ne 0 ]
then
    exit -1
fi

# MA (adult mouse anatomy)
curl -o $ODIR/MA.obo http://www.berkeleybop.org/ontologies/ma.obo

if [ $? -ne 0 ]
then
    exit -1
fi


python $BASEDIR/bin/libdump/GXDAnatomyDumper.py $ODIR/MA.obo $ODIR/EMAP.obo $ODIR/EMAPX.obo


# Adult Mouse Anatomy
#curl -o $ODIR/AdultMouseAnatomy.obo ftp://ftp.informatics.jax.org/pub/reports/adult_mouse_anatomy.obo

# ChEBI - Chemical Entities of Biological Interest
#curl -o $ODIR/ChEBI.obo ftp://ftp.ebi.ac.uk/pub/databases/chebi/ontology/chebi.obo

