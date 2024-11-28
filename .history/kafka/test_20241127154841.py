from hdfs import InsecureClient

# Connexion au serveur HDFS
client = InsecureClient('http://localhost:50070', user='hdfs')

# Chemin du fichier local
local_file = '/home/ghislain/data/myfile.json'

# Chemin du fichier dans HDFS
hdfs_path = '/user/ghislain/json_data/myfile.json'

# Téléverser le fichier local dans HDFS
client.upload(hdfs_path, local_file)