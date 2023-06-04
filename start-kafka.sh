cd kafka
nohup bin/zookeeper-server-start.sh -daemon config/zookeeper.properties > /dev/null 2>&1 &
sleep 2
sudo netstat -tnlp 1>/dev/null | grep -ai '9092' ; # Assuming port 9092 is empty

while [ $? -ne 0 ]
do
   nohup bin/kafka-server-start.sh -daemon config/server.properties > /dev/null 2>&1 &
   sleep 2
done

sudo netstat -tnlp | grep -E '2181|9092'