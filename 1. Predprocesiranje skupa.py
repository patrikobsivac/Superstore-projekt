import pandas as pd

"""
1. Skripta za predprocesiranje skupa podataka
Ova skripta učitava CSV, vrši osnovno čišćenje i prilagođavanje podataka,
te deli skup na 80% i 20% za dalju upotrebu.
"""

# Putanja do CSV datoteke
CSV_FILE_PATH = "superstore.csv"

# Učitavanje CSV datoteke
df = pd.read_csv(CSV_FILE_PATH, delimiter=',')
print("CSV size before: ", df.shape)

# Primer čišćenja i prilagođavanja:

# 1. Uklanjanje nepotrebnih kolona ako ih ima (primer: ako želite ukloniti 'Global_Orders_ID')
# df = df.drop(columns=['Global_Orders_ID'])

# 2. Popunjavanje ili uklanjanje redova sa nedostajućim vrednostima
df = df.dropna()
print("CSV size after dropna: ", df.shape)

# 3. Standardizacija vrednosti u kolonama ako je potrebno
# Na primer, zamena skraćenica u 'Country' koloni (ako ih ima)
df['Country'] = df['Country'].replace({'USA': 'United States', 'UK': 'United Kingdom'})

# 4. Skraćivanje predugačkih tekstualnih polja (npr. Product_Name na 255 karaktera)
df['Product_Name'] = df['Product_Name'].astype(str).str.slice(0, 255)

# 5. Konverzija datuma u datetime format
df['Order_Date'] = pd.to_datetime(df['Order_Date'])
df['Ship_Date'] = pd.to_datetime(df['Ship_Date'])

# 6. Random podela skupa podataka na 80% i 20%
df20 = df.sample(frac=0.2, random_state=1)
df80 = df.drop(df20.index)
print("CSV size 80%: ", df80.shape)
print("CSV size 20%: ", df20.shape)

# 7. Čuvanje predprocesiranih podataka u nove CSV datoteke
df80.to_csv("superstore_80_PROCESSED.csv", index=False)
df20.to_csv("superstore_20.csv", index=False)
print("Predprocesiranje završeno.")
