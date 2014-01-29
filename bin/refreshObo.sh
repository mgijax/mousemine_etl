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

# EMAPA (Abstract Mouse anatomy)
curl ftp://ftp.informatics.jax.org/pub/custom/EMAPA.obo | python $BASEDIR/bin/libdump/ppEmapa.py > $ODIR/EMAPA.obo
e=$?; if [ $e -ne 0 ]; then exit $e; fi

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
#curl -o $ODIR/uberon.obo http://obo.svn.sourceforge.net/svnroot/obo/uberon/trunk/uberon.obo
#e=$?; if [ $e -ne 0 ]; then exit $e; fi

