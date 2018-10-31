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