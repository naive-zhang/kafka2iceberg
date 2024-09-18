mvn clean package -DskipTests
sudo docker cp ./flink-app/target/flink-app-1.0-SNAPSHOT.jar bigdata-docker-flink-1:/root