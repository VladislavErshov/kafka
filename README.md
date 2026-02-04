- Запустить docker-compose
- Запустить Main
- Подключиться к docker
```
docker exec -it kafka-Kafka-1 /bin/bash
```
- Запросить информацию по topic
```
kafka-topics --bootstrap-server localhost:9092 --list
```
```
kafka-topics --bootstrap-server localhost:9092 --describe --topic <topic-name>
```