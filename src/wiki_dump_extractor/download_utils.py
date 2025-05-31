import urllib.request
import os
from tqdm.auto import tqdm


def download_file(url, filepath, replace=False):
    """Download a web file to a filepath, with the option to skip."""
    if os.path.exists(filepath) and not replace:
        print(f"{filepath} already exists, skipping download.")
        return
    
    print(f"Downloading {filepath} from {url}...")
    
    # Get file size for progress bar
    response = urllib.request.urlopen(url)
    total_size = int(response.headers.get('content-length', 0))
    
    # Create progress bar instance
    progress_bar = tqdm(total=total_size, unit='iB', unit_scale=True)
    
    def reporthook(blocknum, blocksize, totalsize):
        progress_bar.update(blocksize)
    
    try:
        urllib.request.urlretrieve(url, filepath, reporthook=reporthook)
    finally:
        progress_bar.close()
    
    print("Download complete")
