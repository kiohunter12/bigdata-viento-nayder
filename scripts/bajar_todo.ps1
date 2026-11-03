docker compose -f pyspark/compose.yml -f pyspark/compose.kafka.yml down
docker compose -f obs/compose.yml down
docker compose -f uso-viento/compose.yml down
docker compose -f kafka/compose.yml down
