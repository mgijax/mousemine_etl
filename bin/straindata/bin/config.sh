
export ENSEMBLRELEASE=91
export BASEURL=ftp://ftp.ensembl.org/pub/release-${ENSEMBLRELEASE}/gff3
#
export BDIR="${DIR}/bin"
export WDIR="${DIR}/work"
export DDIR="${DIR}/downloads"
export GDIR="${DIR}/gff3"
export SDIR="${DIR}/gff3.samples"
#
export LOGFILE="${WDIR}/LOG"
#
export STRAINS="${BDIR}/strains.tsv"
export MAPPINGFILE="${WDIR}/mgiMapping.tsv"
export VALIDIDFILE="${WDIR}/mgiIds.tsv"
#
source "${BDIR}/utils.sh"

