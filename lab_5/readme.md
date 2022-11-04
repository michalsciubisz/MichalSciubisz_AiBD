# Ćwiczenie 2 - TIER protocol i tidy data

Celem ćwiczenia jest pobranie i uporządkowanie przydzielonego zbioru danych zgodnie z protokołem TIER i zasadami "tidy data".

Dla przypomnienia:

- W uporządkowanych danych:
  - Każda zmienna tworzy kolumnę.
  - Każda obserwacja tworzy rząd.
  - Każdy typ jednostki obserwacyjnej tworzy tabelę.

-  [TIER Protocol](https://www.projecttier.org/tier-protocol/tier-protocol-version-history/specifications-3-0/#overview-of-the-documentation/)

 Mamy 5 zbiorów danych.

 1. weather.txt - dane pogodowe. Dzienne dane pogodowe z Global Historical Climatology Network dla jednej stacji pogodowej (MX17004) w Meksyku przez pięć miesięcy w 2010 roku. Pierwsza kolumna zawiera dane (id, rok, miesiąc,nazwa zmiennych), w pozostałych kolumnach są wartości zmiennych na dany dzien miesiąca (dzień, d1 – d31). Miesiące z mniej niż 31 dni mają strukturalne brakujące wartości dla ostatniego dnia (dni) miesiąca. 
 2. tb.csv - dane o gruźlicy w różnych grupach pacjentów (tabela koduje jednocześnie informacje o wieku i o płci w kolumnach i zawiera dużo pustych miejsc)
 3. billboard.csv - notowania billboardu (w tabeli jest data wejścia na listę i ranking w kolejnych tygodniach, nie da się wprost odczytać rankingu w danym tygodniu kalendarzowym i jest dużo pustych miejsc)
 4. drinks.csv Dane i informacje: https://fivethirtyeight.com/features/dear-mona-followup-where-do-people-drink-the-most-beer-wine-and-spirits/
 5. earthquake_data.csv https://fivethirtyeight.com/features/the-rock-isnt-alone-lots-of-people-are-worried-about-the-big-one/ (należy zmienić nazwy kolumn i przygotować tabelę łączącą wiek, płeć i odpowiedź na pytanie "Do you think the "Big One" will occur in your lifetime?"

Użyj [Pandas](https://pandas.pydata.org/docs/).

Numer swojego datasetu uzyskujemy ze wzoru **(N mod 5) +1**, gdzie N to liczba liter w nazwisku.
