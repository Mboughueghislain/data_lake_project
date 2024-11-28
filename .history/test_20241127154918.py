from hdfs import InsecureClient

# Connexion au serveur HDFS
client = InsecureClient('http://localhost:50070', user='hdfs')

# Chemin du fichier local
local_file = '/home/ghislain/Efrei/Data_Integeration/projet_data_lake/data/social_media_2024-11-27 14:46:01.153350.json'

# Chemin du fichier dans HDFS
hdfs_path = '/user/ghislain/json_data/myfile.json'

# Téléverser le fichier local dans HDFS
client.upload(hdfs_path, local_file)