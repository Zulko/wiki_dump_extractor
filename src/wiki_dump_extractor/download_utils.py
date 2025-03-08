import urllib.request
import os


def download_file(url, filepath, replace=False):
    """Download a web file to a filepath, with the option to skip."""
    if os.path.exists(filepath) and not replace:
        print(f"{filepath} already exists, skipping download.")
        return
    print(f"Downloading {filepath}...")
    urllib.request.urlretrieve(url, filepath)
    print("Download complete")
