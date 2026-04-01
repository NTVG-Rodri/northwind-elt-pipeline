import requests
import psycopg2

url = "https://api.open-meteo.com/v1/forecast?latitude=-32.48&longitude=-58.23&current_weather=true"
data = requests.get(url).json()

temp = data["current_weather"]["temperature"]
wind = data["current_weather"]["windspeed"]

conn = psycopg2.connect(
    host='postgres_db',
    database='mydatabase',
    user='root',
    password='root'
)

cursor = conn.cursor()

cursor.execute("""
            INSERT INTO weather (temperature, windspeed)
            VALUES (%s, %s)   
            """, (temp, wind))

conn.commit()
conn.close()

print('datos guardados')
