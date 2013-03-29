# BASE
BASEDIR=..

ODIR=$BASEDIR/output/panther
if [ ! -d $ODIR ]
then
    mkdir -p $ODIR
fi

curl -o $ODIR/RefGenomeOrthologs.tar.gz ftp://ftp.pantherdb.org/ortholog/8.0/RefGenomeOrthologs.tar.gz 

tar -xzvf $ODIR/RefGenomeOrthologs.tar.gz -C $ODIR

mv $ODIR/RefGenomeOrthologs $ODIR/RefGenomeOrthologs.txt

rm $ODIR/RefGenomeOrthologs.tar.gz
if [ $? -ne 0 ]
then
    exit $?
fi

