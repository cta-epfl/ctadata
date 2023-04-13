import os
import time
from urllib.parse import urljoin
import requests
import logging
from .util import urljoin_multipart
from . import __version__


logger = logging.getLogger(__name__)

class StorageException(Exception):
    pass

# TODO: move to bravado and rest
class APIClient:
    __export_functions__ = ['list_dir', 'fetch_and_save_file', 'upload_file', 'upload_dir']
    __class_args__ = ['token', 'downloadservice', 'data_root', 'optional_url_parts', 'chunk_size']

    downloadservice = os.getenv("CTADS_URL", "http://hub:5000/services/downloadservice/")
    optional_url_parts = ["services/downloadservice/"]
    
    chunk_size = 10 * 1024 * 1024

    @property
    def token(self):
        if not hasattr(self, "_token"):
            self._token = os.getenv("JUPYTERHUB_API_TOKEN")
        
        return self._token
    
    @token.setter
    def token(self, value):
        self._token = value


    def construct_endpoint_url(self, endpoint, path):        
        # downloadservice = self.downloadservice.replace("http://", "https://")
        downloadservice = self.downloadservice

        return urljoin_multipart(downloadservice, endpoint, path)
    

    def get_endpoint(self, endpoint, path, stream=False, chunk_size=None):
        full_url = self.construct_endpoint_url(endpoint, path)
            
        logger.info("full url: %s", full_url)
        
        params = {'token': self.token, 'ctadata_version': __version__, 'chunk_size': chunk_size}

        return requests.get(full_url, params=params, stream=stream)
        

    def list_dir(self, path, token=None, downloadservice=None):
        r = self.get_endpoint('list', path)

        if r.status_code != 200:
            logger.error("error: %s", r.text)
            raise StorageException(r.text)

        
        try:
            return r.json()                        
        except Exception as e:
            logger.error("error: %s", e)
            raise


    def fetch_and_save_file(self, path, save_to_fn=None):
        fileinfo = self.get_endpoint('list', path)
        if fileinfo.status_code not in [200, 207]:
            logger.error("error: %s", fileinfo.text)
            raise StorageException(fileinfo.text)
        
        fileinfo = fileinfo.json()[0]
        filesize = int(fileinfo['size'])

        total_wrote = 0

        if save_to_fn is None:
            save_to_fn = path.split("/")[-1]

        last_pc = 0

        t0 = time.time()

        with open(save_to_fn, "wb") as out_file:
            # with get(url, token, downloadservice, stream=True) as f:          
            f = self.get_endpoint('fetch', path, stream=True, chunk_size=self.chunk_size)

            logger.info("got response %s", f)
            f.raise_for_status()
            i_chunk = 0
            
            for r in f.iter_content(chunk_size=self.chunk_size):
                pc = int(total_wrote / filesize * 100)
                if pc > last_pc:
                    logger.info("wrote %.2f / %.2f Mb in %d chunks in %.2f seconds", total_wrote/1024/1024, filesize/1024/1024, i_chunk, time.time() - t0)
                    last_pc = pc
                    
                out_file.write(r)
                total_wrote += len(r)
                i_chunk += 1

        return total_wrote
    

    def upload_file(self, local_fn, path):
        url = self.construct_endpoint_url('upload', path)
        logger.info("uploading %s to %s", local_fn, url)

        with open(local_fn, "rb") as f:
            stats = {'total_size': 0}

            def generate(stats):
                while r := f.read(self.chunk_size):
                    stats['total_size'] += len(r)
                    logger.info("uploaded %s Mb", stats['total_size']/1024/1024)
                    yield r

            r = requests.post(url, data=generate(stats), params={'token': self.token, 'ctadata_version': __version__}, stream=True)
            
        if r.status_code != 200:
            logger.error("error: %s", r.text)
            raise StorageException(r.text)

        logger.info("upload result: %s %s", r, r.json())
        return r.json()
    
    def upload_dir(self, local_dir, path):
        logger.info("uploading dir %s to %s", local_dir, path)
        for (dirpath, dirnames, filenames) in os.walk(local_dir):
            for name in filenames:
                fn = os.path.join(dirpath, name)
                self.upload_file(fn, os.path.join(path, dirpath[len(local_dir):], name))


api_client = APIClient()
