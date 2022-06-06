# kafka-demo

# Dependancy
Kafka 2.8

JDK 17

mvn

# Kafka
docker-compose up -d

create a topic called NewTopic

# GUI tool
OffsetExporer 2.2

mvn clean compile install

mvn springboot:run

curl http://localhost:8081/publish/message/no-of-replicas

# ref
https://www.baeldung.com/ops/kafka-docker-setup

# Kafaka Streams

Need below dependancies

./kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic my-kafka-left-stream-topic

./kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic my-kafka-right-stream-topic
