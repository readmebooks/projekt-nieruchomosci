import os

# Ten skrypt dokumentuje pochodzenie danych o nieruchomościach (UK Price Paid Data)
# Plik został pobrany ręcznie z: 
# http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv

file_path = "data/raw/pp-complete.csv"

if os.path.exists(file_path):
    size_gb = os.path.getsize(file_path) / (1024**3)
    print(f"Dane o nieruchomościach są gotowe w folderze data/raw.")
    print(f"Rozmiar pliku: {size_gb:.2f} GB")
else:
    print("BŁĄD: Nie znaleziono pliku pp-complete.csv w folderze data/raw/")
    print("Pobierz go ręcznie i umieść w odpowiedniej lokalizacji.")