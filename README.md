# Client for CTA Data Download Service at Swiss CTA DC

## Disclaimer

This is a prototype development only for Swiss CTA DC. Please report bugs here, and for any details please inquire the CTAO CH DC management.

## Purpose

CTA data are/will be stored at CTAO-CH data center at CSCS.
These data should be accessible for selected external users.
In addition, these files should be available within interactive analysis platform at CSCS.
This client presents an API access to these services and data.
## Installation

```bash
$ pip install 'ctadata>=0.2.6'
```

## Usage from within CTA CSCS JupyterHub platform

From within CTA CSCS JupyterHub platform, selected authorized users are able to access the "data download service", like so:

```python
import ctadata

for url in ctadata.list_dir("cta/DL1/20241114/v0.1/"):
    if 'datacheck' not in url and '.0100' in url and '11111' in url:
        print("stored", ctadata.fetch_and_save_file(url)/1024/1024, "Mb")
        print("found keys", h5py.File(url.split("/")[-1]).keys())
```

To download a file:

```python
ctadata.fetch_and_save_file_or_dir("lst/some-data-dir", recursive=True)
```

or, in bash and recursively:

```bash
ctadata get --recursive lst/some-data-dir
```

### Uploading files

To upload a file:

```python
ctadata.upload_file("latest", "filelists/latest-file-list")
```

The result is:

```json
{
    "path": "lst/users/volodymyr_savchenko_epfl_ch/filelists/latest-file-list",
    "status": "uploaded",
    "total_written": 60098730
}
```

Note that for every user, the file is uploaded to their own directory constructed from the user name. The path specified is relative to this directory. If you need to move the files to common directories, please as support.  But you likely want to just share returned path to be used as so:

```python
ctadata.fetch_and_save_file("lst/users/volodymyr_savchenko_epfl_ch/filelists/latest-file-list")
```

You can also use command line interface to do this:

```bash
$ cta-data put latest-file-list latest-file-list-bla-bla
```

**Beware that all the files written are accessible to all CTAO members and all platform users.**

## From outside (possibly another jupyterhub)

You need to get yourself a jupyterhub token, it will be used to authenticate to the download service.

If you are in the session, navigate to the hub control panel this way:

![image](https://user-images.githubusercontent.com/3909535/227050172-35318c23-c138-40cb-b6ce-d2f6e780fa06.png)

Request a token:

![image](https://user-images.githubusercontent.com/3909535/227050281-2b012c15-ab84-4d75-a961-85057440fcf4.png)

The rest is similar to the previous case:

```python
import os
os.environ["CTADS_URL"] = "DATA-DISTRIBUTING-JUPYTERHUB/services/downloadservice/"
os.environ["JUPYTERHUB_API_TOKEN"] = "INSERT-YOUR-TOKEN-HERE"

for url in ctadata.list_dir("cta/DL1/20241114/v0.1"):
    if 'datacheck' not in url and '.0100' in url and '11111' in url:
        print("stored", ctadata.fetch_and_save_file(url)/1024/1024, "Mb")
        print("found keys", h5py.File(url.split("/")[-1]).keys())
```

# Webdav Client

In order to make use of bare WebDAV interface of the storage, `ctadata` also provides a configured `webdav4` client (see [webdav4](https://github.com/skshetry/webdav4) for documentation).

```python
client = ctadata.webdav4_client()
client.ls("/")
client.uploadFile("example.txt", "remote/example.txt")
```

Please see [WebDAV4 documenation](https://skshetry.github.io/webdav4/) for details on it's wide range of features.

# Delegating a proxy grid certificate to the Platform

In order to make use of your own grid certificate to access CTA-CSCS storage from within CTA interactive platform it is necessary to upload you short-term proxy certificate to the platform. `cta-data` provides an easy way to do this:

This tools also offers a way to upload your own time limited certificate to access the background webdav server.

```python
import ctadata
ctadata.upload_certificate('yourcertificate.crt')
```

Note that if you do not upload your own certificate, you can ask to make use of a shared robot certificate used for data syncing.
