from dataload.uploader import SnpeffPostUpdateUploader
from .cgi_parser import load_data


class CgiUploader(SnpeffPostUpdateUploader):
    name = "cgi"
    __metadata__ = {"mapper": 'observed',
                    "assembly": "hg19",
                    "src_meta": {
                        "url": "https://www.cancergenomeinterpreter.org/home",
                        "license_url": "https://creativecommons.org/publicdomain/zero/1.0/",
                        "license_url_short": "https://goo.gl/pBW3vj",
                        "licence": "CC0 1.0 Universal",
                    }
                    }

    def load_data(self,data_folder):
        # there's one vcf file there, let's get it
        input_file = os.path.join(data_folder,"cgi_biomarkers_per_variant.tsv")
        assert os.path.exists(input_file), "Can't find input file '%s'" % input_file
        self.logger.info("Load data from file '%s'" % input_file)
        return load_data(input_file)

    @classmethod
    def get_mapping(self):
        mapping = {
            "cgi": {
                "properties": {
                    'association': {
                        'analyzer': 'string_lowercase',
                        'type': 'string'
                    },
                    'cdna': {
                        'analyzer': 'string_lowercase',
                        'type': 'string'
                    },
                    'drug': {
                        'analyzer': 'string_lowercase',
                        'type': 'string'
                    },
                    'evidence_level': {
                        'analyzer': 'string_lowercase',
                        'type': 'string'
                    },
                    'gene': {
                        'analyzer': 'string_lowercase',
                        'type': 'string'
                    },
                    'primary_tumor_type': {
                        'analyzer': 'string_lowercase',
                        'type': 'string'
                    },
                    'protein_change': {
                        'analyzer': 'string_lowercase',
                        'type': 'string'
                    },
                    'region': {
                        'analyzer': 'string_lowercase',
                        'type': 'string'
                    },
                    'source': {
                        'analyzer': 'string_lowercase',
                        'type': 'string'
                    },
                    'transcript': {
                        'analyzer': 'string_lowercase',
                        'type': 'string'
                    }
                }
            }
        }
        
        return mapping