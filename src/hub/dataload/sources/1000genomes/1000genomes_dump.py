__author__ = 'm088378'
import os
import os.path
import sys
import biothings, config
biothings.config_for_app(config)

from config import DATA_ARCHIVE_ROOT, logger as logging
from biothings.hub.dataload.dumper import FTPDumper

### Raymond's Notes
# REFDATA_PATH/ALL.wgs.phase3_shapeit2_mvncall_integrated_v5b.20130502.sites.vcf.gz
# ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/ALL.wgs.phase3_shapeit2_mvncall_integrated_v5b.20130502.sites.vcf.gz
# REFDATA_PATH/ALL.chrMT.phase3_callmom-v0_4.20130502.genotypes.vcf.gz "This will be cat'd with the main file"
# ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/ALL.chrMT.phase3_callmom-v0_4.20130502.genotypes.vcf.gz

class KGenomeDumper(FTPDumper):
    SRC_NAME = "1000genomes"
    SRC_ROOT_FOLDER = os.path.join(DATA_ARCHIVE_ROOT, SRC_NAME)
    FTP_HOST = 'ftp://ftp.1000genomes.ebi.ac.uk'
    CWD_DIR = '/vol1/ftp/release/20130502/'
    #FILE_RE = 'All_\d{8}.vcf.gz'
    MT_VCF = 'ALL.chrMT.phase3_callmom-v0_4.20130502.genotypes.vcf.gz'
    WGS_VCF = 'ALL.wgs.phase3_shapeit2_mvncall_integrated_v5b.20130502.sites.vcf.gz'

    SCHEDULE = "0 9 * * *"


    def create_todump_list(self, force=False):
        #ALL.chrMT.phase3_callmom-v0_4.20130502.genotypes.vcf.gz
        new_localfile = os.path.join(self.new_data_folder,os.path.basename(self.WGS_VCF))
        try:
            current_localfile = os.path.join(self.current_data_folder,os.path.basename(self.WGS_VCF))
        except TypeError:
            # current data folder doesn't even exist
            current_localfile = new_localfile
        if force or not os.path.exists(current_localfile) or self.remote_is_better(self.WGS_VCF, current_localfile):
            self.to_dump.append({"remote":self.WGS_VCF, "local":new_localfile})

    def post_dump(self, *args, **kwargs):
        self.logger.info("Uncompressing files in '%s'" % self.new_data_folder)
        unzipall(self.new_data_folder)

def main():
    dumper = KGenomeDumper()
    dumper.dump()

if __name__ == "__main__":
    main()
