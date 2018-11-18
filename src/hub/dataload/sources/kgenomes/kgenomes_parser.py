__author__ = 'raymond301'
import vcf
from config import DATA_ARCHIVE_ROOT, logger as logging
from biothings.utils.dataload import dict_sweep, unlist, value_convert_to_number
from utils.hgvs import get_hgvs_from_vcf

def get_Ancestral_Allele(aa_str):
    aa_arr = aa_str.split("\|")
    aaAllele = aa_arr[0]
    aaType = aa_arr[3]
    if aaAllele == "" or aaAllele == "-" or aaAllele == ".":
        aaAllele = None
    if aaType == "" or aaType == "-" or aaType == ".":
        aaType = None
    return aaAllele,aaType


def _map_line_to_json(doc_key, item):
    chrom = item.CHROM
    chromStart = item.POS
    ref = item.REF
    info = item.INFO
    _filter = item.FILTER
    try:
        vt = info['VT']
    except:
        vt = None
    try:
        ancestral_allele,ancestral_indel_type = get_Ancestral_Allele( info['AA'] )
    except:
        ancestral_allele = None
        ancestral_indel_type = None
    try:
        exonFlag = info['EX_TARGET']
    except:
        exonFlag = None
    # convert vcf object to string
    item.ALT = [str(alt) for alt in item.ALT]
    # if multiallelic, put all variants as a list in multi-allelic field
    hgvs_list = None
    if len(item.ALT) > 1:
        hgvs_list = [get_hgvs_from_vcf(chrom, chromStart, ref, alt, mutant_type=False) for alt in item.ALT]
    for i, alt in enumerate(item.ALT):
        (HGVS, var_type) = get_hgvs_from_vcf(chrom, chromStart, ref, alt, mutant_type=True)
        if HGVS is None:
            return
        assert len(item.ALT) == len(info['AC']), "Expecting length of item.ALT= length of info.AC, but not for %s" % (HGVS)
        assert len(item.ALT) == len(info['AF']), "Expecting length of item.ALT= length of info.AF, but not for %s" % (HGVS)
        one_snp_json = {
            "_id": HGVS,
            doc_key : {
                "chrom": chrom,
                "pos": chromStart,
                "filter": _filter,
                "multi-allelic": hgvs_list,
                "ref": ref,
                "alt": alt,
                "alleles": item.ALT,
                "type": var_type,
                "ac": info['AC'][i],
                "an": info['AN'][i],
                "af": {
                    "af": info['AF'][i],
                    "af_afr": info['AFR_AF'][i],
                    "af_amr": info['AMR_AF'][i],
                    "af_eas": info['EAS_AF'][i],
                    "af_eur": info['EUR_AF'][i],
                    "af_sas": info['SAS_AF'][i]
                },
                "dp": info['DP'],
                "vt": vt,
                "num_samples": info['NS'],
                "aa": {
                    "ancestral_allele": ancestral_allele,
                    "ancestral_indel_type": ancestral_indel_type
                },
                "exon_flag": exonFlag,
            }
        }
        obj = (dict_sweep(unlist(value_convert_to_number(one_snp_json)), [None]))
        yield obj


################################################################
# This is REQUIRED function called load_data.                  #
# It takes input file or a input folder as the first input,    #
# and output a list or a generator of structured JSON objects  #
################################################################

def load_data(doc_key,input_file):
    vcf_reader = vcf.Reader(open(input_file, 'r'))
    for record in vcf_reader:
        ### Need to Skip CNV & SV records, avoid bad HGVSg definitions, that aren't queriable
        try:
            svtype = record.INFO["SVTYPE"]
            skip = True
        except:
            skip = False

        if skip:
            logging.info("Skip SV Record: %s : %s > %s [%s]".format(record.CHROM, record.POS, record.ALT, svtype))
            continue

        for record_mapped in _map_line_to_json(doc_key,record):
            yield record_mapped
