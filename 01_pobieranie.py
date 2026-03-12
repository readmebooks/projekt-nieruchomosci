import requests
import zipfile
import os

url = "https://f001.backblazebase.com/file/Backblaze-Hard-Drive-Data/data_Q1_2023.zip"
cel = "data/raw/pobrane_dane.zip"

print("Pobieram dane (ok. 1.2 GB)... To potrwa chwilę, zaparz kawę.")
r = requests.get(url, stream=True)
with open(cel, 'wb') as f:
    for chunk in r.iter_content(chunk_size=1024*1024):
        f.write(chunk)

print("Rozpakowuję (z 1.2 GB zrobi się ok. 10.5 GB plików CSV)...")
with zipfile.ZipFile(cel, 'r') as zip_ref:
    zip_ref.extractall("data/raw/")

os.remove(cel)
print("Gotowe! Dane czekają w folderze data/raw")