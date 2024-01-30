import os
import urllib.request
import tarfile
from tqdm import tqdm

def download_data_with_progress(url):
    extract_path = "tmp"

    # Create the directory if it doesn't exist
    os.makedirs(extract_path, exist_ok=True)

    # Construct the file path for the downloaded tar.gz file
    file_path = os.path.join(extract_path, os.path.basename(url))

    # Download the file with progress bar
    with tqdm(unit='B', unit_scale=True, unit_divisor=1024, miniters=1, desc="Downloading") as t:
        urllib.request.urlretrieve(url, file_path, reporthook=lambda blocknum, blocksize, total_size: download_progress_hook(t, blocknum, blocksize, total_size))

    # Extract the contents of the tar.gz file
    with tarfile.open(file_path, 'r:gz') as tar:
        tar.extractall(path=extract_path)

def download_progress_hook(t, blocknum, blocksize, total_size):
    """
    A hook function to update the tqdm progress bar during download.
    """
    t.update(blocknum * blocksize - t.n)

def load_otf2(app="ping-pong", num_procs=2):
    available = {
        "ping-pong": [2],
        "laghos": [32],
        "tortuga": [16, 32, 64, 128, 256],
    }

    if app not in available:
        raise ValueError(f"Application {app} not available")
    
    if num_procs not in available[app]:
        raise ValueError(f"Number of processors {num_procs} not available for application {app}")
    
    identifier = f"{app}_{num_procs}_otf2"

    download_data_with_progress(f"https://archive.org/download/{identifier}.tar/{num_procs}.tar.gz")

    
def load_projections(app="ping-pong", num_procs=2):
    available = {
        "ping-pong": [2],
        "loimos": [64, 128],
    }

    if app not in available:
        raise ValueError(f"Application {app} not available")
    
    if num_procs not in available[app]:
        raise ValueError(f"Number of processors {num_procs} not available for application {app}")
    
    identifier = f"{app}_{num_procs}_otf2"

    download_data_with_progress(f"https://archive.org/download/{identifier}.tar/{num_procs}.tar.gz")