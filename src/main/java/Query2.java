import Utils.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class Query2 {

    public static void main(String[] args) {

        Utils.preProcessData(Utils.FILE_Q2,Utils.PRE_PROCESS_TEMPLATE2);
        Utils.preProcessData(Utils.PARQUET_FILE_Q2, Utils.PARQUET_TEMPLATE2);
        //init SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("Query2")
                .getOrCreate();

        Dataset<Row> dataframe = spark
                .read()
                .option("header", "true")
                .parquet("hdfs://namenode:9000/home/dataset-batch/Query2.parquet");




        //inizio query
        long start = System.currentTimeMillis();

        //estrarre il campo corretto da 'vault_id' causa conversione csv parquet
        dataframe = dataframe.withColumn("vault_id", col("vault_id.member0"));



        Dataset<Row> failuresPerModel = dataframe
                .groupBy("model")
                .agg(sum("failure.member0").alias("total_failures"));

        //ordinare in ordine crescente in base a 'total_failures'
        Dataset<Row> sortedRows = failuresPerModel.orderBy(desc("total_failures")).limit(10);

        sortedRows.show(Integer.MAX_VALUE);

        //selezionare solo 'vault_id' e 'models'
        Dataset<Row> selectedColumns = dataframe.select("vault_id", "model", "failure.member0");

        //filtrare per failure = 1
        Dataset<Row> failedRows = selectedColumns.filter(selectedColumns.col("member0").equalTo("1"));

        //groupBy 'vault_id' + calcolare tot fallimenti per ogni vault
        Dataset<Row> vaultFailures = failedRows
                .groupBy("vault_id")
                .agg(count("member0").alias("total_failures"));

        //per ogni 'vault_id', raccogliere modelli distinti hard disk failed
        Dataset<Row> vaultModels = failedRows
                .groupBy("vault_id")
                .agg(collect_set("model").alias("unique_models"));

        //join
        Dataset<Row> joinedDF = vaultFailures.join(vaultModels, "vault_id");

        //creazione classifica
        Dataset<Row> secondRanking = joinedDF
                .orderBy(col("total_failures").desc())
                .limit(10);

        secondRanking.show(10, false);

        //fine query
        System.out.println("Query 2 elapsed time: " + (System.currentTimeMillis()-start) + "ms\n");

        //causa scrittura converto la lista di models in stringa
        Dataset<Row> secondRankingString = secondRanking.withColumn("unique_models",
                functions.concat_ws(",", secondRanking.col("unique_models")));

        String outputFilePath = "hdfs://namenode:9000/output/query2/";

        //scrittura su HDFS
        sortedRows.write()
                .format("csv")
                .option("header", "true")
                .mode("overwrite")
                .save(outputFilePath);

        secondRankingString.write()
                .format("csv")
                .option("header", "true")
                .mode("overwrite")
                .save("hdfs://namenode:9000/output/query2.2/");


        //risultati sul volume locale
        sortedRows.write().format("csv").option("header", "true").mode("overwrite").save("file:///home/results/output2/");
        secondRankingString.write().format("csv").option("header", "true").mode("overwrite").save("file:///home/results/output2.2/");

        java.util.List<Row> rows = sortedRows.collectAsList();

        //scrivere righe in Redis
        try (Jedis jedis = new Jedis("redis-cache", 6379)) {
            for (Row row : rows) {
                String model = row.getAs("model");
                Object totalFailuresObj = row.getAs("total_failures");

                Long totalFailures;
                if (totalFailuresObj instanceof Double) {
                    totalFailures = ((Double) totalFailuresObj).longValue();
                } else {
                    totalFailures = (Long) totalFailuresObj;
                }

                jedis.hset("query2", model, String.valueOf(totalFailures));
            }
        }


        java.util.List<Row> rows2 = secondRanking.collectAsList();

        try (Jedis jedis = new Jedis("redis-cache", 6379)) {
            for (Row row : rows2) {
                int vaultId = row.getInt(0);
                String totalFailures = String.valueOf(row.getLong(1));
                List<String> uniqueModelsList = row.getList(row.fieldIndex("unique_models"));
                String uniqueModels = uniqueModelsList.stream().collect(Collectors.joining(", "));

                Map<String, String> hash = new HashMap<>();
                hash.put("vaultId", String.valueOf(vaultId));
                hash.put("totalFailures", totalFailures);
                hash.put("uniqueModels", uniqueModels);

                String redisKey = "query2.2:" + vaultId;

                jedis.hset(redisKey, hash);


            }
        }


        spark.stop();
    }


}
