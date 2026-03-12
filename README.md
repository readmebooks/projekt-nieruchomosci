Dokumentacja projektu - UK Price Paid Data
Opis projektu

Projekt realizuje proces przetwarzania danych transakcyjnych rynku nieruchomości w Wielkiej Brytanii (UK Price Paid Data). System został zaprojektowany w architekturze medalionowej (Bronze, Silver, Gold) z wykorzystaniem podejścia ELT (Extract, Load, Transform). Głównym celem było obsłużenie wolumenu danych przekraczającego 10 GB przy zachowaniu wysokiej wydajności przetwarzania.

Specyfikacja techniczna

-Silnik bazy danych: DuckDB (In-process OLAP)

-Środowisko: Python 3.14 / VS Code

-Kontrola wersji: Git (lokalnie)

-Wolumen danych: ~10.6 GB (62 000 000 rekordów w warstwie Bronze)

Potok danych (Medallion Architecture)

1. Warstwa Bronze (Raw): Zawiera surowe dane załadowane bezpośrednio z plików CSV. W celu przetestowania wydajności silnika oraz spełnienia wymogów projektowych, dane zostały zduplikowane (UNION ALL), co pozwoliło na symulację pracy ze zbiorem o rozmiarze 10.6 GB.

2. Warstwa Silver (Cleaned): Warstwa przetworzona, w której dokonano deduplikacji rekordów (eliminacja duplikatów z warstwy Bronze). Przeprowadzono rzutowanie typów danych (CAST) oraz mapowanie technicznych nazw kolumn na nazwy biznesowe.

3. Warstwa Gold (Curated): Skondensowana tabela wynikowa zawierająca zagregowane statystyki (średnia cena, liczba transakcji) w podziale na miasta.

Identyfikacja problemów z jakością danych

W trakcie prac zidentyfikowano trzy kluczowe wyzwania:

1. Redundancja danych: Wynikająca z błędu zasilania lub specyfiki źródeł (zduplikowane wiersze). Rozwiązane poprzez zastosowanie klauzuli DISTINCT w procesie transformacji do warstwy Silver.

2. Niejawny schemat (brak nagłówków): Pliki źródłowe nie posiadają nazw kolumn. Rozwiązane poprzez jawne mapowanie indeksów technicznych (column00-column14) na atrybuty logiczne w warstwie Silver.

3. Niespójność formatów dat: Surowe daty zawierają niepotrzebne znaczniki czasowe (timestamp). Zoptymalizowano to poprzez przycięcie ciągów znaków i rzutowanie na natywny typ DATE.

Instrukcja uruchomienia

1. Zainstalować DuckDB: pip install duckdb.

2. Umieścić plik pp-complete.csv w lokalizacji data/raw/.

3. Uruchomić skrypt procesowy: python 02_elt_proces.py.

4. Weryfikacja bazy: projekt tworzy plik nieruchomosci_uk.db z kompletem tabel.