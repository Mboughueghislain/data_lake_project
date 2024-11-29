# Se mettre dans le dossier "data_integration_kafka1" afin de executer les commandes

# lancer le serveur ssh
ssh localhost

# Démarrez NameNode et DataNode
start-dfs.sh

# lancer le zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# lancer le serveur kafka
~/Téléchargements/kafka37$ bin/kafka-server-start.sh config/server.properties 
                    ou
bin/kafka-server-start.sh config/server.properties

# lancer le spark Shell 
spark-shell

# Lancer l'API flask 
python api.py

# lancer le random_data pour générer les données
python random_data.py

# lancer le staging.py pour traitrer les fichiers générés
python staging.py

# End points
http://127.0.0.1:5000/data_json/all

http://127.0.0.1:5000/data_csv/all

http://127.0.0.1:5000/data_txt/all
