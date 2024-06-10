import Utils.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class Query1 {

    public static void main(String[] args) {

        Utils.preProcessData(Utils.FILE_Q1,Utils.PRE_PROCESS_TEMPLATE1);
        Utils.preProcessData(Utils.PARQUET_FILE_Q1, Utils.PARQUET_TEMPLATE1);

        SparkSession spark = SparkSession.builder()
                .appName("Query1")
                .master("spark://spark-master:7077")
                .getOrCreate();

        //inizio query
        long start = System.currentTimeMillis();

        // Leggi il parquet in un DataFrame
        Dataset<Row> data = spark
                .read()
                .option("header", "true")
                .parquet("hdfs://namenode:9000/home/dataset-batch/Query1.parquet");
        
                //.cache();  // Cache the DataFrame to reuse it multiple times

        System.out.println("Avvenuta lettura csv: " + (System.currentTimeMillis()-start) + "ms\n");

        // Converte la colonna "date" nel formato DateType
        data = data.withColumn("data", to_date(col("data"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"));


        // Converte la colonna "failure" in IntegerType
        data = data.withColumn("failure.member0", col("failure.member0").cast("int"));

        // Calcola il numero totale di fallimenti per ogni giorno e per ogni vault_id
        Dataset<Row> failuresCount = data.groupBy("data", "vault_id.member0")
                .agg(sum("failure.member0").alias("failures_count"));
                //.cache();  // Cache the result to speed up the filtering steps

        // Filtra i vault che hanno subito esattamente 4, 3 e 2 fallimenti
        Dataset<Row> filteredFailures = failuresCount.filter(col("failures_count").isin(2, 3, 4));
                //.cache();  // Cache the result to reuse it later

        // Ordina per data e vault_id
        Dataset<Row> sortedFilteredFailures = filteredFailures.orderBy("data", "member0");

        // Formatta la colonna "data" nel formato "DD-MM-YYYY"
        sortedFilteredFailures = sortedFilteredFailures.withColumn("data", date_format(col("data"), "dd-MM-yyyy"));

        // Mostra i risultati
        sortedFilteredFailures.show();

        //fine query
        System.out.println("Query 1 elapsed time: " + (System.currentTimeMillis()-start) + "ms\n");

        sortedFilteredFailures = sortedFilteredFailures.withColumnRenamed("member0", "vault_id");
        // Salva i risultati su HDFS sovrascrivendo i dati esistenti
        String outputFilePath = "hdfs://namenode:9000/output/query1/";
        sortedFilteredFailures.write().format("csv").option("header", "true").mode("overwrite").save(outputFilePath);

        // Scrivi i risultati sul volume locale
        sortedFilteredFailures.write().format("csv").option("header", "true").mode("overwrite").save("file:///home/results/output1/");
        java.util.List<Row> rows = sortedFilteredFailures.collectAsList();


        // Scrivi le righe in Redis
        try (Jedis jedis = new Jedis("redis-cache", 6379)) { // Modifica l'host e la porta se necessario
            for (Row row : rows) {

                // Estrai i valori dalla riga
                String date = row.getString(row.fieldIndex("data"));
                int vaultId = row.getInt(row.fieldIndex("vault_id"));
                String failuresCountString = String.valueOf(row.getLong(row.fieldIndex("failures_count")));

                // Crea una mappa con i valori da scrivere su Redis
                Map<String, String> hash = new HashMap<>();
                hash.put("date", date);
                hash.put("vaultId", String.valueOf(vaultId));
                hash.put("failuresCount", failuresCountString);

                // Usa una chiave unica per ogni riga, ad esempio combinando date e vaultId
                String redisKey = "query1:" + date + ":" + vaultId;

                // Inserisci i valori in Redis come un hash
                jedis.hset(redisKey, hash);
            }
            System.out.println("Dati scritti su Redis con successo");
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Ferma la sessione Spark
        spark.stop();
    }
}
