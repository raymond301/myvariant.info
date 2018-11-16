__author__ = 'raymond301'
import glob, os
from .kgenomes_parser import load_data
import biothings.hub.dataload.uploader as uploader
from hub.dataload.uploader import SnpeffPostUpdateUploader

class KgenomesBaseUploader(SnpeffPostUpdateUploader):
    __metadata__ = {"mapper" : 'observed',
                    "assembly" : "hg19",
                    "src_meta" : {
                        "url" : "http://exac.broadinstitute.org/",
                        "license" : "ODbL",
                        "license_url" : "http://exac.broadinstitute.org/terms",
                        "license_url_short": "nnn"
                    }
                    }


class KgenomesUploader(KgenomesBaseUploader):
    name = "kgenomes"
    main_source= "1000genomes"

    def load_data(self,data_folder):
        content = glob.glob(os.path.join(data_folder,"*.genotypes.vcf"))
        if len(content) != 1:
            raise uploader.ResourceError("Expecting one single vcf file, got: %s" % repr(content))
        input_file = content.pop()
        self.logger.info("Load data from file '%s'" % input_file)
        return load_data(self.__class__.name, input_file)

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
