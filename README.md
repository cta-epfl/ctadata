# Client for CTA Data dCache storage access

## Disclaimer

This is a prototype development only for Swiss CTA DC. Please report bugs here, and for any details please inquire the CTAO CH DC management.

## Purpose

CTA data are/will be stored at CTAO-CH data center at CSCS.
These data should be accessible for selected external users.
In addition, these files should be available within interactive analysis platform at CSCS.
This client presents an API access to these services and data.

# Accessing dCache with tokens

The latest version of ctadata implements direct download/upload mode without using downloadservice as proxy.

## Installation

Currently the direct API uses [oidc-agent](https://indigo-dc.gitbook.io/oidc-agent) tool for the token maintanance and [davix](https://github.com/cern-fts/davix) tools for downloading and uploading the files. These tools can be either compiled locally and added to PATH environment variable or installed inside [micromamba] (https://mamba.readthedocs.io/en/latest/user_guide/micromamba.html) or [conda] (https://anaconda.org/anaconda/conda) environment. Below is the example of the installation using micromamba package manager.

```bash
$ micromamba create -n ctadata python oidc-agent davix  # create micromamba environment with python and required binaries
$ micromamba activate ctadata # activate the environment
$ pip install ctadata # install ctadata library in the same environment
```

##  Token maintanance

To get access to CTA-CSCS storage you need to generate  OpenID Connect token. The token is temporary and needs to be updated on regular bases. This process is implemented by the token agent service which can be started by the command

```bash
$ cta-data start-agent
```

The token is being stored in the user's home directory, so the above command needs to be started only on a single machine. During the agent initialization process an account is created for the token maintanance. The user is being asked to authenticate the account creation by visiting the authentication page in a browser on any device.

## Basic usage

As soon as the account creation process completes the token is created and one can start using other direct API functions.

### Listing directory contents

```python
import ctadata

for path in ctadata.list_dir("cta"):
    print(path)
```

### Downloading files and directories

To download contents of some file or dir:

```python
import ctadata

# downloading single file
ctadata.fetch_and_save_file_or_dir("lst/some-data-dir/some-data-file") 

# recursively downloading a directory
ctadata.fetch_and_save_file_or_dir("lst/some-data-dir", recursive=True)
```

or, in bash:

```bash
cta-data get lst/some-data-dir/some-data-file
cta-data get --recursive lst/some-data-dir
```

### Uploading files and directories

To upload a file:

```python
import ctadata
ctadata.upload_file("latest.txt", "your-folder/new-file-name.md")
ctadata.upload_file("latest.txt", "your-folder/") # will autocomplete to `your-folder/latest.txt`
```
You can also use command line interface to do this:

```bash
$ cta-data put latest-file-list latest-file-list-bla-bla
```

### Using token in external tools

You can print the token to use it with external tools with the following command

```bash
$ cta-data print-token
```

### Using dev instance of dCache server

To access dev instance one may use `-d` or `--dev` option in command interface:

```bash
$ cta-data -d list /
```

In python API one should use `dev_instance` optional parameter:

```python
import ctadata

for path in ctadata.list_dir("/", dev_instance=True):
    print(path)
```

Note that in order to access dev instance server you will have to maintain separate token using agent service, which can be started with command:
```bash
$ cta-data -d start-agent
```
## Usage from within CTA CSCS JupyterHub platform

The ctadata library can be used within the CTA CSCS JupyterHub platform, as described above, without any changes or limitations.
