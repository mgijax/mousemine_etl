
export ENSEMBL_GFFURL=ftp://ftp.ensembl.org/pub/current_gff3
export ENSEMBL_FASTAURL=ftp://ftp.ensembl.org/pub/current_fasta
export ENS_VER=93
export ENS_DNA="dna" # dna, dna_sm, or dna_rm
export ENS_DNA_OBJ="toplevel"
export ENS_GFF3="ftp://ftp.ensembl.org/pub/release-${ENS_VER}/gff3"
export ENS_FASTA="ftp://ftp.ensembl.org/pub/release-${ENS_VER}/fasta"

export MGI_GFF3=http://www.informatics.jax.org/downloads/mgigff3/MGI.gff3.gz

export BDIR="${DIR}"
if [ "${ODIR}" == "" ] ; then
    export ODIR="${DIR}/output"
    export WDIR="${ODIR}/work"
    export DDIR="${ODIR}/downloads"
    export GDIR="${ODIR}/gff3"
    export SDIR="${ODIR}/gff3.samples"
    export FDIR="${ODIR}/fasta"
else
    export WDIR="${ODIR}/../work"
    export DDIR="${ODIR}/../downloads"
    export GDIR="${ODIR}/gff3"
    export SDIR="${ODIR}/../gff3.samples"
    export FDIR="${ODIR}/fasta"
fi
#
if [ "${LOGFILE}" == "" ] ; then
    export LOGFILE="${WDIR}/LOG"
fi
#
export DGDIR="${DDIR}/gff3"
export DFDIR="${DDIR}/fasta"
#
export STRAINS="${BDIR}/strains.tsv"
export MAPPINGFILE="${WDIR}/mgiMapping.tsv"
export VALIDIDFILE="${WDIR}/mgiIds.tsv"
#
source "${BDIR}/utils.sh"

