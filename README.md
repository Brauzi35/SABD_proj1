# SABD proj1

This project aims to analyze hard disk data to compute statistics various statistics. For further details please refer to the provided docs.

__Authors__

* :man_technologist: Brauzi Valerio (matricola 0333768)
* :woman_technologist: Calcaura Marina (matricola 0334044)

## Prerequisites

Before you begin, ensure you have met the following requirements:
- Docker installed on your machine
- Docker Compose installed on your machine
- Maven installed on your machine
- An internet connection
- At least 15GB avaliable (images + containers)

## Getting Started

### Setting Up the Environment

1. Clone the repository
2. Compile the project to create the .jar file:
    ```sh
    mvn clean package
    ```
   
    

3. Build and run the Docker containers:
    ```sh
    docker compose up -d --scale spark-worker=x --scale datanode=y
    ```
    Replace `x` with the desired number of Spark worker nodes.
    Replace `y` with the desired number of HDFS Datanodes.

### Running the Queries

To execute one of the predefined queries (1 to 3), use the following command:
```sh
docker-compose exec spark spark-submit --master spark://spark-master:7077 --class QueryX /opt/bitnami/spark/app/SABD_proj1-1.0-SNAPSHOT.jar
```
To run Query 1:
```sh
docker-compose exec spark spark-submit --master spark://spark-master:7077 --class Query1 /opt/bitnami/spark/app/SABD_proj1-1.0-SNAPSHOT.jar
```
To run Query 2:
```sh
docker-compose exec spark spark-submit --master spark://spark-master:7077 --class Query2 /opt/bitnami/spark/app/SABD_proj1-1.0-SNAPSHOT.jar
```
To run Query 3:
```sh
docker-compose exec spark spark-submit --master spark://spark-master:7077 --class Query3 /opt/bitnami/spark/app/SABD_proj1-1.0-SNAPSHOT.jar
```
Please check your .jar name, in case of custum maven config.

### Notes
Once the containers are up and running, verify that nifi is ready (it can take from 10s to 80s, depending on your operating machine and OS).


