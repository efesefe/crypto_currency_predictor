cd kafka
bin/kafka-server-stop.sh config/server.properties
sleep 2
bin/zookeeper-server-stop.sh config/zookeeper.properties