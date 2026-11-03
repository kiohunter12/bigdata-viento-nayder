# Crea los topics del proyecto (Kafka tiene auto-create desactivado)
$kt = "/opt/kafka/bin/kafka-topics.sh"
docker exec lambda26-kafka $kt --create --if-not-exists --topic viento-eventos --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
docker exec lambda26-kafka $kt --create --if-not-exists --topic alerta-eventos --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1
docker exec lambda26-kafka $kt --describe --bootstrap-server kafka:9092 --topic viento-eventos
docker exec lambda26-kafka $kt --describe --bootstrap-server kafka:9092 --topic alerta-eventos
