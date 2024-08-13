import random
from faker import Faker
import time
from pymongo import MongoClient

mongo_address = "mongodb://localhost:27017/"
database_name = "SensorData"
collection_name = "Sensors"

# Połączenie z MongoDB
client = MongoClient(mongo_address)
db = client[database_name]
collection = db[collection_name]

# Tworzenie instancji Faker z polską lokalizacją
fake = Faker('pl_PL')

# Maksymalna liczba rekordów (dokumentów .json) danych
max_rows = 4000

# Lista unikalnych ID czujników (sensorów)
sensor_ids = list(range(1, max_rows + 1))

# Lista do przechowywania wygenerowanych danych
data_list = []

# Rozpoczynamy generowanie danych
row_count = 0
while row_count < max_rows:
    # Dodanie prefiksu do ID czujnika
    sensor_id = "P3_" + str(sensor_ids.pop(random.randint(0, len(sensor_ids) - 1)))

    # Generujemy fałszywe dane czujnika w języku polskim z dodatkowymi informacjami
    sensor_data = {
        "id_czujnika": sensor_id,                               # Identyfikator sensora z prefiksem
        "miasto": fake.city(),                                  # Miasto
        "data_pomiaru": fake.date_time_this_year().strftime('%Y-%m-%d %H:%M:%S'),  # Data i godzina pomiaru
        "temperatura": round(fake.pyfloat(min_value=20, max_value=30, right_digits=2), 2),  # Temperatura
        "wilgotność": round(fake.pyfloat(min_value=40, max_value=60, right_digits=2), 2),    # Wilgotność
        "ciśnienie": round(fake.pyfloat(min_value=900, max_value=1100, right_digits=2), 2)    # Ciśnienie
    }

 # Wstawiamy dane do kolekcji MongoDB
    collection.insert_one(sensor_data)
    
    print("Wstawione dane do MongoDB:", sensor_data)

    # Inkrementujemy licznik wierszy danych
    row_count += 1

    # Opcjonalnie: Czekamy przed wygenerowaniem kolejnych danych (np. co 0.01 sekundy)
    time.sleep(0.02)


print(f"Wstawiono {max_rows} rekordów do kolekcji '{collection_name}' w bazie danych '{database_name}'.")



