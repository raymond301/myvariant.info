__author__ = 'raymond301'
from collections import OrderedDict
import copy
import time
from vcf import Reader
from utils.common import timesofar

__METADATA__ = {
    "requirements": [
        "PyVCF>=0.6.7",
    ],
    "src_name": '1000genomes',
    "src_url": 'ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/',
    "version": 'phase3_v5b.20130502',
    "field": "1000genomes"
}


################################################################
# This is REQUIRED function called load_data.                  #
# It takes input file or a input folder as the first input,    #
# and output a list or a generator of structured JSON objects  #
################################################################

# split_keys="AC,AF,AFR_AF,AMR_AF,EAS_AF,EUR_AF,SAS_AF,VT"
# gunzip -c  $sites_vcf  $mt_vcf_noheader_gz \
# | perl  $COMMON/VCF_split.pl  -k $split_keys \
# | bior_vcf_to_tjson  --logfile $TEMP_DIR/bior.log.bior_vcf_to_tjson  2>$TEMP_DIR/stderr.bior_vcf_to_tson \
# | bior_modify_tjson -f modify_tjson.config  --logfile $TEMP_DIR/bior.log.bior_modify_tjson  2>$TEMP_DIR/stderr.bior_modify_tjson > $TEMP_DIR/intermediate.tjson
# cat $TEMP_DIR/intermediate.tjson | sort -T $TEMP_DIR/srtTmp -k 1,1 -k 2,2n -k 4,4 -k 5,5 > $TEMP_DIR/intermediate.sort.tjson
# ruby merge_duplicates.rb $TEMP_DIR/intermediate.sort.tjson > $MAKE_JSON_OUTPUT_FILE_PATH


def load_data(infile):
    chrom_list = [str(i) for i in range(1, 23)] + ['X', 'Y', 'MT']
    for chrom in chrom_list:
        print("Processing chr{}...".format(chrom))
        #snpdoc_iter = parse_vcf(infile, compressed=True, verbose=False, by_id=True, reference=chrom)
        #for doc in snpdoc_iter:
        #    _doc = {'dbsnp': doc}
        #    _doc['_id'] = doc['_id']
        #    del doc['_id']
        #    yield _doc


