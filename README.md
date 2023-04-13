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
$ pip install 'ctadata>=0.1.14'
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

### Uploading files

To upload a file:

```python
ctadata.upload_file("latest", "filelists/latest-file-list")
```

The result is:

```
{'path': 'lst/users/volodymyr_savchenko_epfl_ch/filelists/latest-file-list',
 'status': 'uploaded',
 'total_written': 60098730}
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

