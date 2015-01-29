#!/usr/bin/env python

import os
import dropbox
import tempfile
from pprint import pformat
from execo import Timer
from execo_engine import Engine, sweep, ParamSweeper, igeom, slugify, logger


class DroxpboxBench(Engine):
    """ """

    def create_file(self, size):
        """ """
        fd, fname = tempfile.mkstemp()
        with os.fdopen(fd, 'w') as fout:
            fout.write(os.urandom(size))
        return fname

    def upload_file_sdk(self, client, filePath, fileName):
        """Upload a file's content.
        Args:
            service: Drive API service instance.
            filePath : Path to the file you want to upload.
            fileName : Name of the new file in the drive.
        """
        f = open(filePath, 'rb')
        response = client.put_file(fileName, f)
        return response

    def download_file_sdk(self, client, fileName, filePath):
        """Download a file's content.
        Args:
            client: Dropbox client instance.
            fileName: Name of the file you want to download.
            filePath: Name of the new local file.
        """
        f, _ = client.get_file_and_metadata(fileName)
        out = open(filePath, 'wb')
        out.write(f.read())
        out.close()
        return True

    def run(self):
        """ """
        token = 'bRIJb9jp5igAAAAAAAAACc5QzQ619Vp0pYa2PdIrt0q2y0qFyJgwrKvtzuTp3Sz_'
        client = dropbox.client.DropboxClient(token)
        parameters = {'size': igeom(128, 2048, 5),
                      'db_if': ['rest', 'sdk']}
        combs = sweep(parameters)
        sweeper = ParamSweeper(self.result_dir + "/sweeps", combs)

        f = open(self.result_dir + '/results.txt', 'w')
        while len(sweeper.get_remaining()) > 0:
            comb = sweeper.get_next()

            logger.info('Treating combination %s', pformat(comb))
            comb_dir = self.result_dir + '/' + slugify(comb)
            try:
                os.mkdir(comb_dir)
            except:
                pass

            fname = self.create_file(comb['size'])

            timer = Timer()

            if comb['db_if'] == 'sdk':
                self.upload_file_sdk(client, fname, fname.split('/')[-1])
                up_time = timer.elapsed()
                self.download_file_sdk(client, fname.split('/')[-1],
                                       comb_dir + fname.split('/')[-1])
                dl_time = timer.elapsed() - up_time
                sweeper.done(comb)
            elif comb['db_if'] == 'rest':
                logger.warning('REST interface not implemented')
                sweeper.skip(comb)
                continue
            os.remove(fname)
            f.write("%f %i %f %f \n" % (timer.start_date(), comb['size'],
                                        up_time, dl_time))
        f.close()

if __name__ == "__main__":
    e = DroxpboxBench()
    e.start()
