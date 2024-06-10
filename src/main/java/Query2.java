import Utils.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class Query2 {

    public long executeQuery2(){

        Utils.preProcessData(Utils.FILE_Q2,Utils.PRE_PROCESS_TEMPLATE2);
        Utils.preProcessData(Utils.PARQUET_FILE_Q2, Utils.PARQUET_TEMPLATE2);


        long start = System.currentTimeMillis();

        SparkSession spark = SparkSession
                .builder()
                .appName("Query2")
                .getOrCreate();

        Dataset<Row> dataframe = spark
                .read()
                .option("header", "true")
                .parquet("hdfs://namenode:9000/home/dataset-batch/Query2.parquet");// Imposta il flag header a true se il CSV ha una riga di intestazione
        //.csv("hdfs://namenode:9000/home/dataset-batch/Query2.csv"); // Modifica il percorso in base al tuo file CSV

        // Estrai il campo corretto da 'vault_id'
        dataframe = dataframe.withColumn("vault_id", col("vault_id.member0"));



        Dataset<Row> failuresPerModel = dataframe
                .groupBy("model")
                .agg(sum("failure.member0").alias("total_failures"));

        // Ordina le righe in ordine crescente in base al valore della colonna 'total_failures'
        Dataset<Row> sortedRows = failuresPerModel.orderBy(desc("total_failures")).limit(10);

        sortedRows.show(Integer.MAX_VALUE);

        // Seleziona solo le colonne 'vault_id' e 'models'
        Dataset<Row> selectedColumns = dataframe.select("vault_id", "model", "failure.member0");

        // Filtra solo le righe con failure = 1
        Dataset<Row> failedRows = selectedColumns.filter(selectedColumns.col("member0").equalTo("1"));

        // Raggruppa per 'vault_id' e calcola il numero totale di fallimenti per ogni vault
        Dataset<Row> vaultFailures = failedRows
                .groupBy("vault_id")
                .agg(count("member0").alias("total_failures"));

        // Per ogni 'vault_id', raccogli i modelli distinti di hard disk soggetti ad almeno un fallimento
        Dataset<Row> vaultModels = failedRows
                .groupBy("vault_id")
                .agg(collect_set("model").alias("unique_models"));

        // Unisci i due DataFrame
        Dataset<Row> joinedDF = vaultFailures.join(vaultModels, "vault_id");

        // Ordina in base al numero totale di fallimenti in ordine decrescente e seleziona i primi 10
        Dataset<Row> secondRanking = joinedDF
                .orderBy(col("total_failures").desc())
                .limit(10);

        // Visualizza i risultati
        secondRanking.show(10, false);

        long end = (System.currentTimeMillis()-start);

        // Concatena gli elementi dell'array in una stringa
        Dataset<Row> secondRankingString = secondRanking.withColumn("unique_models",
                functions.concat_ws(",", secondRanking.col("unique_models")));

        // Percorso del file CSV di output su HDFS
        String outputFilePath = "hdfs://namenode:9000/output/";

        // Scrivi il DataFrame su HDFS in formato CSV
        sortedRows.write()
                .format("csv")
                .option("header", "true")
                .mode("overwrite")
                .save(outputFilePath);

        secondRankingString.write()
                .format("csv")
                .option("header", "true")
                .mode("overwrite")
                .save(outputFilePath);


        // Scrivi i risultati sul volume locale
        sortedRows.write().format("csv").option("header", "true").mode("overwrite").save("file:///home/results/output2/");
        secondRankingString.write().format("csv").option("header", "true").mode("overwrite").save("file:///home/results/output2.2/");

        // Converti il DataFrame in una lista di righe
        java.util.List<Row> rows = sortedRows.collectAsList();
        // Scrivi le righe in Redis

        try (Jedis jedis = new Jedis("redis-cache", 6379)) { // Assicurati che il nome del servizio sia corretto
            // Itera attraverso la lista di righe e salva i dati su Redis
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

        // Converti il DataFrame in una lista di righe
        java.util.List<Row> rows2 = secondRanking.collectAsList();
        // Scrivi le righe in Redis

        try (Jedis jedis = new Jedis("redis-cache", 6379)) { // Assicurati che il nome del servizio sia corretto
            // Itera attraverso la lista di righe e salva i dati su Redis
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

        return end;
    }
    public static void main(String[] args) {

        Utils.preProcessData(Utils.FILE_Q2,Utils.PRE_PROCESS_TEMPLATE2);
        Utils.preProcessData(Utils.PARQUET_FILE_Q2, Utils.PARQUET_TEMPLATE2);
        // Inizializza SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("Query2")
                .getOrCreate();

        Dataset<Row> dataframe = spark
                .read()
                .option("header", "true")
                .parquet("hdfs://namenode:9000/home/dataset-batch/Query2.parquet");// Imposta il flag header a true se il CSV ha una riga di intestazione
                //.csv("hdfs://namenode:9000/home/dataset-batch/Query2.csv"); // Modifica il percorso in base al tuo file CSV



        //inizio query
        long start = System.currentTimeMillis();

        // Estrai il campo corretto da 'vault_id'
        dataframe = dataframe.withColumn("vault_id", col("vault_id.member0"));



        Dataset<Row> failuresPerModel = dataframe
                .groupBy("model")
                .agg(sum("failure.member0").alias("total_failures"));

        // Ordina le righe in ordine crescente in base al valore della colonna 'total_failures'
        Dataset<Row> sortedRows = failuresPerModel.orderBy(desc("total_failures")).limit(10);

        sortedRows.show(Integer.MAX_VALUE);

        // Seleziona solo le colonne 'vault_id' e 'models'
        Dataset<Row> selectedColumns = dataframe.select("vault_id", "model", "failure.member0");

        // Filtra solo le righe con failure = 1
        Dataset<Row> failedRows = selectedColumns.filter(selectedColumns.col("member0").equalTo("1"));

        // Raggruppa per 'vault_id' e calcola il numero totale di fallimenti per ogni vault
        Dataset<Row> vaultFailures = failedRows
                .groupBy("vault_id")
                .agg(count("member0").alias("total_failures"));

        // Per ogni 'vault_id', raccogli i modelli distinti di hard disk soggetti ad almeno un fallimento
        Dataset<Row> vaultModels = failedRows
                .groupBy("vault_id")
                .agg(collect_set("model").alias("unique_models"));

        // Unisci i due DataFrame
        Dataset<Row> joinedDF = vaultFailures.join(vaultModels, "vault_id");

        // Ordina in base al numero totale di fallimenti in ordine decrescente e seleziona i primi 10
        Dataset<Row> secondRanking = joinedDF
                .orderBy(col("total_failures").desc())
                .limit(10);

        // Visualizza i risultati
        secondRanking.show(10, false);

        //fine query
        System.out.println("Query 2 elapsed time: " + (System.currentTimeMillis()-start) + "ms\n");

        // Concatena gli elementi dell'array in una stringa
        Dataset<Row> secondRankingString = secondRanking.withColumn("unique_models",
                functions.concat_ws(",", secondRanking.col("unique_models")));

        // Percorso del file CSV di output su HDFS
        String outputFilePath = "hdfs://namenode:9000/output/query2/";

        // Scrivi il DataFrame su HDFS in formato CSV
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


        // Scrivi i risultati sul volume locale
        sortedRows.write().format("csv").option("header", "true").mode("overwrite").save("file:///home/results/output2/");
        secondRankingString.write().format("csv").option("header", "true").mode("overwrite").save("file:///home/results/output2.2/");

        // Converti il DataFrame in una lista di righe
        java.util.List<Row> rows = sortedRows.collectAsList();
        // Scrivi le righe in Redis

        try (Jedis jedis = new Jedis("redis-cache", 6379)) { // Assicurati che il nome del servizio sia corretto
            // Itera attraverso la lista di righe e salva i dati su Redis
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

        // Converti il DataFrame in una lista di righe
        java.util.List<Row> rows2 = secondRanking.collectAsList();
        // Scrivi le righe in Redis

        try (Jedis jedis = new Jedis("redis-cache", 6379)) { // Assicurati che il nome del servizio sia corretto
            // Itera attraverso la lista di righe e salva i dati su Redis
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
