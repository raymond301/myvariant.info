__author__ = 'raymond301'
import os
import os.path
import re, random
import biothings, config
biothings.config_for_app(config)

from config import DATA_ARCHIVE_ROOT, logger as logging
from biothings.hub.dataload.dumper import FTPDumper
from biothings.utils.common import gunzipall

### Raymond's Notes
# REFDATA_PATH/ALL.wgs.phase3_shapeit2_mvncall_integrated_v5b.20130502.sites.vcf.gz
# ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/ALL.wgs.phase3_shapeit2_mvncall_integrated_v5b.20130502.sites.vcf.gz
# REFDATA_PATH/ALL.chrMT.phase3_callmom-v0_4.20130502.genotypes.vcf.gz "This will be cat'd with the main file"
# ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/ALL.chrMT.phase3_callmom-v0_4.20130502.genotypes.vcf.gz

class KGenomeDumper(FTPDumper):
    SRC_NAME = "kgenomes"
    SRC_ROOT_FOLDER = os.path.join(DATA_ARCHIVE_ROOT, 'kgenomes')
    FTP_HOST = 'ftp.1000genomes.ebi.ac.uk'
    CWD_DIR = '/vol1/ftp/release/' # want 20130502 release
    FILE_PATTERN = ".*\.genotypes\.vcf\.gz$"
    SCHEDULE = None # No new release is scheduled by group


    def get_newest_info(self):
        # so we need to parse directory names
        releases = self.client.nlst()
        # sort items based on k
        #logging.debug("Yup - "+str(random.randint(1,10)))
        self.release = sorted(releases)[-1]
        contents = self.client.nlst(self.release)
        pat = re.compile(self.__class__.FILE_PATTERN)
        self.newest_files = [f for f in contents if pat.match(f)]

    def new_release_available(self):
        current_release = self.src_doc.get("download",{}).get("release")
        if not current_release or self.release > current_release:
            self.logger.info("New release '%s' found" % self.release)
            return True
        else:
            self.logger.debug("No new release found")
            return False

    def create_todump_list(self, force=False):
        self.get_newest_info()
        # for filename in self.newest_files[13:14]: # use this for debugging, will only pull the chr21 file
        for filename in self.newest_files:
            new_localfile = os.path.join(self.new_data_folder,os.path.basename(filename))
            try:
                current_localfile = os.path.join(self.current_data_folder,os.path.basename(filename))
            except TypeError:
                # current data folder doesn't even exist
                current_localfile = new_localfile
            if force or not os.path.exists(current_localfile) or self.remote_is_better(filename,current_localfile) or self.new_release_available():
                # register new release (will be stored in backend)
                self.release = self.release
                self.to_dump.append({"remote": filename,"local":new_localfile})

    def post_dump(self, *args, **kwargs):
        self.logger.info("Uncompressing files in '%s'" % self.new_data_folder)
        gunzipall(self.new_data_folder)


def main():
    dumper = KGenomeDumper()
    dumper.dump()

if __name__ == "__main__":
    main()
