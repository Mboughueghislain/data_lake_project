from django.shortcuts import render
import pandas as pd
from django.http import JsonResponse
from myproject.settings import hdfs_client

def get_parquet_data(request, file_path):
    """
    API pour lire un fichier Parquet depuis HDFS et retourner les données au format JSON.
    :param request: Requête HTTP
    :param file_path: Chemin du fichier Parquet dans HDFS sans le préfixe 'hdfs://localhost:9092'
    :return: JsonResponse
    """
    try:
        # Ajouter le préfixe HDFS à file_path
        hdfs_file_path = f'hdfs://localhost:9092/{file_path}'
        
        # Lire le fichier Parquet depuis HDFS
        with hdfs_client.read(hdfs_file_path) as reader:
            df = pd.read_parquet(reader)

        # Convertir les données en JSON
        data = df.to_dict(orient='records')
        return JsonResponse(data, safe=False)

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)
