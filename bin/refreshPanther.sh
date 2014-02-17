# BASE
BASEDIR=..

ODIR=$BASEDIR/output/panther
if [ ! -d $ODIR ]
then
    mkdir -p $ODIR
fi

curl -o $ODIR/RefGenomeOrthologs.tar.gz ftp://ftp.pantherdb.org/ortholog/current_release/RefGenomeOrthologs.tar.gz 
e=$?; if [ $e -ne 0 ]; then exit $e; fi

tar -xzvf $ODIR/RefGenomeOrthologs.tar.gz -C $ODIR

mv $ODIR/RefGenomeOrthologs $ODIR/RefGenomeOrthologs.txt

rm $ODIR/RefGenomeOrthologs.tar.gz

