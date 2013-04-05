# BASE
BASEDIR=..

ODIR=$BASEDIR/output/homologene
if [ ! -d $ODIR ]
then
    mkdir -p $ODIR
fi

curl -o $ODIR/homologene.data ftp://ftp.ncbi.nih.gov/pub/HomoloGene/current/homologene.data

if [ $? -ne 0 ]
then
    exit $?
fi
