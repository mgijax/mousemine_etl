
export ENSEMBLRELEASE=91
export BASEURL=ftp://ftp.ensembl.org/pub/release-${ENSEMBLRELEASE}/gff3
#
export BDIR="${DIR}/bin"
if [ "${ODIR}" == "" ] ; then
    export ODIR="${DIR}/output"
    export WDIR="${ODIR}/work"
    export DDIR="${ODIR}/downloads"
    export GDIR="${ODIR}/gff3"
    export SDIR="${ODIR}/gff3.samples"
else
    export WDIR="${ODIR}/../work"
    export DDIR="${ODIR}/../downloads"
    export GDIR="${ODIR}"
    export SDIR="${ODIR}/../gff3.samples"
fi
#
if [ "${LOGFILE}" == "" ] ; then
    export LOGFILE="${WDIR}/LOG"
fi
#
export STRAINS="${BDIR}/strains.tsv"
export MAPPINGFILE="${WDIR}/mgiMapping.tsv"
export VALIDIDFILE="${WDIR}/mgiIds.tsv"
#
source "${BDIR}/utils.sh"

