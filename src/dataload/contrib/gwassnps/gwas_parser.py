from __future__ import print_function
import os
import sys
import string
import json
import gc

import MySQLdb
import requests


def load_data(step=1000, offset=0):
    MySQLHG19 = MySQLdb.connect('genome-mysql.cse.ucsc.edu',
                                db='hg19', user='genomep', passwd='password')
    Cursor = MySQLHG19.cursor()

    # get the row number of gwasCatalog
    sql = "SELECT COUNT(*) FROM gwasCatalog"
    Cursor.execute(sql)
    numrows = Cursor.fetchone()[0]
    print(numrows)

    sql = "SELECT * FROM gwasCatalog"
    Cursor.execute(sql)

    for i in range(numrows):
        snp = Cursor.fetchone()
        if i and i % step == 0:
            print(i)

        chrom = snp[1]
        chrom = chrom[3:]
        chromStart = int(snp[2]) + 1
        chromEnd = int(snp[3]) + 1
        rsid = snp[4]
        pubMedID = snp[5]
        trait = snp[9]
        riskAllele = snp[14]
        pValue = snp[16]
        # parse from myvariant.info to get hgvs_id, ref, alt information based on rsid
        url = 'http://localhost:8000/v1/query?q=dbsnp.rsid:'\
               + rsid + '&fields=_id,dbsnp.ref,dbsnp.alt,dbsnp.chrom,dbsnp.hg19'
        r = requests.get(url)
        for hits in r.json()['hits']:
            HGVS = hits['_id']
            
            one_snp_json = {
            # plus 'gwas'
                "_id": HGVS,
                "gwassnp":
                    {
                        "rsid": rsid,
                        "pubmed": pubMedID,
                        "trait": trait,
                        "risk_allele": riskAllele,
                        "pvalue": pValue,
                    }
            }
            yield one_snp_json