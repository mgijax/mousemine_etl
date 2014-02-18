# BASE
BASEDIR=..

ODIR=$BASEDIR/output/panther
if [ ! -d $ODIR ]
then
    mkdir -p $ODIR
fi

curl -o $ODIR/RefGenomeOrthologs.tar.gz ftp://ftp.pantherdb.org/ortholog/8.1/RefGenomeOrthologs.tar.gz 
e=$?; if [ $e -ne 0 ]; then exit $e; fi

tar -xzvf $ODIR/RefGenomeOrthologs.tar.gz -C $ODIR
# Panther release 8.1 has a typo in the file name
if [ -e $ODIR/RefGeneomeOrthologs ]
then
    mv $ODIR/RefGeneomeOrthologs $ODIR/RefGenomeOrthologs.txt
else
    mv $ODIR/RefGenomeOrthologs $ODIR/RefGenomeOrthologs.txt
fi

rm $ODIR/RefGenomeOrthologs.tar.gz

