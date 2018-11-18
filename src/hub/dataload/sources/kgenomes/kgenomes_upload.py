__author__ = 'raymond301'
import itertools, glob, os
from .kgenomes_parser import load_data
import biothings.hub.dataload.uploader as uploader
from hub.dataload.uploader import SnpeffPostUpdateUploader

class KgenomesBaseUploader(uploader.IgnoreDuplicatedSourceUploader,
                           uploader.ParallelizedSourceUploader,
                           SnpeffPostUpdateUploader):

    def jobs(self):
        files = glob.glob(os.path.join(self.data_folder,self.__class__.GLOB_PATTERN))
        if len(files) != 1:
            raise uploader.ResourceError("Expected 1 files, got: %s" % files)
        chrom_list = [str(i) for i in range(1, 23)] + ['X', 'Y', 'MT']
        return list(itertools.product(files,chrom_list))

    def load_data(self,input_file,chrom):
        self.logger.info("Load data from '%s' for chr %s" % (input_file,chrom))
        return load_data(self.__class__.__metadata__["assembly"],input_file,chrom)

    def post_update_data(self, *args, **kwargs):
        super(KgenomesBaseUploader,self).post_update_data(*args,**kwargs)
        self.logger.info("Indexing 'rsid'")
        # background=true or it'll lock the whole database...
        self.collection.create_index("dbsnp.rsid",background=True)

    @classmethod
    def get_mapping(klass):
        mapping = {
            "kgenomes" : {
                "properties": {
                    "chrom": {
                        "type": "text",
                        "analyzer": "string_lowercase"
                    },
                    "pos": {
                        "type": "long"
                    },
                    "ref": {
                        "type": "text",
                        "analyzer": "string_lowercase"
                    },
                    "filter": {
                        "type": "text",
                        "analyzer": "string_lowercase"
                    },
                    "alt": {
                        "type": "text",
                        "analyzer": "string_lowercase"
                    },
                    "multi-allelic": {
                        "type": "text",
                        "analyzer": "string_lowercase"
                    },
                    "alleles": {
                        "type": "text",
                        "analyzer": "string_lowercase"
                    },
                    "type": {
                        "type": "text",
                        "analyzer": "string_lowercase"
                    },
                    "qual": {
                        "type": "float"
                    },
                    "filter": {
                        "type": "text",
                        "analyzer": "string_lowercase"
                    },
                    "ac": {
                        "type": "integer"
                    },
                    "an": {
                        "type": "integer"
                    },
                    "af": {
                        "properties": {
                            "af": {
                                "type": "float"
                            },
                            "af_afr": {
                                "type": "float"
                            },
                            "af_amr": {
                                "type": "float"
                            },
                            "af_eas": {
                                "type": "float"
                            },
                            "af_eur": {
                                "type": "float"
                            },
                            "af_sas": {
                                "type": "float"
                            }
                        }
                    },
                    "dp": {
                        "type": "integer"
                    },
                    "vt": {
                        "type": "text",
                        "analyzer": "string_uppercase"
                    },
                    "num_samples": {
                        "type": "integer"
                    },
                    "dp": {
                        "type": "long"
                    },
                    "exon_flag": {
                        "type": "boolean"
                    },
                    "aa": {
                        "properties": {
                            "ancestral_allele": {
                                "type": "text",
                                "analyzer": "string_lowercase"
                            },
                            "ancestral_indel_type": {
                                "type": "text",
                                "analyzer": "string_lowercase"
                            }
                        }
                    }
                }
            },
        }
        return mapping


class KgenomesUploader(KgenomesBaseUploader):
    name = "kgenomes"
    main_source= "kgenomes"

    __metadata__ = {"mapper" : 'observed',
                    "assembly" : "hg19",
                    "src_meta" : {
                        "url" : "http://www.internationalgenome.org/about",
                        "license" : "Creative Commons",
                        "license_url" : "http://www.internationalgenome.org/announcements/data-management-and-community-access-paper-published-2012-04-29/",
                        "license_url_short": "nnn"
                    }
                    }
    GLOB_PATTERN = ".*\.genotypes\.vcf\.gz$"

