#!/usr/bin/env python3
from kafka import KafkaConsumer
import csv
import os
import json

def main():
    consumer = KafkaConsumer(
        'crypto_prices',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='csv-writer-group'
    )

    output_file = '/app/storage/output.csv'
    
    file_exists = os.path.isfile(output_file)

    with open(output_file, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)

        # Écrire l’en-tête uniquement si le fichier n'existe pas encore
        if not file_exists:
            writer.writerow(['coin', 'spot', 'buy', 'sell', 'spread', 'timestamp'])

        print("Consommation en cours... les messages seront ajoutés dans", output_file)
        for message in consumer:
            try:
                # 1. Décoder le message en UTF-8
                msg_str = message.value.decode('utf-8')

                # 2. Parser le JSON
                data = json.loads(msg_str)

                # 3. Extraire les champs souhaités dans l'ordre voulu
                row = [
                    data['coin'],
                    data['spot'],
                    data['buy'],
                    data['sell'],
                    data['spread'],
                    data['timestamp']
                ]

                # 4. Écrire la ligne CSV
                writer.writerow(row)
                csvfile.flush()  # forcer l'écriture sur disque
            except Exception as e:
                print("Erreur lors du traitement du message:", e)

if __name__ == "__main__":
    main()
