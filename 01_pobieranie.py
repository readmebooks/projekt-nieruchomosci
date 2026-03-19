import os
import urllib.request

# Konfiguracja
URL = "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv"
DATA_DIR = "data/raw"
FILE_PATH = os.path.join(DATA_DIR, "pp-complete.csv")

def download_data():
    # 1. Tworzymy folder, jeśli nie istnieje
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        print(f"Utworzono folder: {DATA_DIR}")

    # 2. Sprawdzamy, czy plik już jest
    if os.path.exists(FILE_PATH):
        size_gb = os.path.getsize(FILE_PATH) / (1024**3)
        print(f"Plik już istnieje: {FILE_PATH} ({size_gb:.2f} GB). Pomijam pobieranie.")
        return

    # 3. Jeśli nie ma - pobieramy
    try:
        print("--- POBIERANIE ---")
        print("To może potrwać chwilę...")
        
        # Pobieranie pliku
        urllib.request.urlretrieve(URL, FILE_PATH)
        
        print("Plik został pobrany pomyślnie!")
    except Exception as e:
        print(f"Wystąpił błąd podczas pobierania: {e}")
        print("Spróbuj pobrać plik ręcznie z powyższego adresu URL.")

if __name__ == "__main__":
    download_data()