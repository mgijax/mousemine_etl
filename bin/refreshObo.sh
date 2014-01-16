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
e=$?; if [ $e -ne 0 ]; then exit $e; fi

python $BASEDIR/bin/libdump/MedicConflater.py $ODIR/MEDIC.obo $ODIR/MEDIC_conflated.obo
e=$?; if [ $e -ne 0 ]; then exit $e; fi


# GO
curl -o $ODIR/GeneOntology.obo http://www.geneontology.org/ontology/obo_format_1_0/gene_ontology.1_0.obo
e=$?; if [ $e -ne 0 ]; then exit $e; fi


# MP
curl -o $ODIR/MammalianPhenotype.obo ftp://ftp.informatics.jax.org/pub/reports/MPheno_OBO.ontology
e=$?; if [ $e -ne 0 ]; then exit $e; fi


# UBERON
curl -o $ODIR/uberon.obo http://obo.svn.sourceforge.net/svnroot/obo/uberon/trunk/uberon.obo
e=$?; if [ $e -ne 0 ]; then exit $e; fi

# EMAP (Edinburgh Anatomy)
curl -o $ODIR/EMAP.obo ftp://ftp.hgu.mrc.ac.uk/pub/MouseAtlas/Anatomy/EMAP_combined.obo
e=$?; if [ $e -ne 0 ]; then exit $e; fi

# MA (adult mouse anatomy)
curl -o $ODIR/MA.obo http://www.berkeleybop.org/ontologies/ma.obo
e=$?; if [ $e -ne 0 ]; then exit $e; fi

python $BASEDIR/bin/libdump/GXDAnatomyDumper.py $ODIR/MA.obo $ODIR/EMAP.obo $ODIR/EMAPX.obo
e=$?; if [ $e -ne 0 ]; then exit $e; fi

