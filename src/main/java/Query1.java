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

        //parquet in un DataFrame
        Dataset<Row> data = spark
                .read()
                .option("header", "true")
                .parquet("hdfs://namenode:9000/home/dataset-batch/Query1.parquet");
        
                //.cache();  // Cache the DataFrame to reuse it multiple times

        System.out.println("Avvenuta lettura parquet: " + (System.currentTimeMillis()-start) + "ms\n");

        //conv "data" nel formato DateType
        data = data.withColumn("data", to_date(col("data"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"));


        //cnv la colonna "failure" in int
        data = data.withColumn("failure.member0", col("failure.member0").cast("int"));

        //calcola il numero totale di fallimenti per ogni giorno e per ogni vault_id
        Dataset<Row> failuresCount = data.groupBy("data", "vault_id.member0")
                .agg(sum("failure.member0").alias("failures_count"));
                //.cache();

        //filtro i vault che hanno subito 4, 3 e 2 fallimenti
        Dataset<Row> filteredFailures = failuresCount.filter(col("failures_count").isin(2, 3, 4));
                //.cache();

        //ordina per data e vault_id
        Dataset<Row> sortedFilteredFailures = filteredFailures.orderBy("data", "member0");

        //formattaz. colonna "data" in "DD-MM-YYYY"
        sortedFilteredFailures = sortedFilteredFailures.withColumn("data", date_format(col("data"), "dd-MM-yyyy"));


        sortedFilteredFailures.show();

        //fine query
        System.out.println("Query 1 elapsed time: " + (System.currentTimeMillis()-start) + "ms\n");

        sortedFilteredFailures = sortedFilteredFailures.withColumnRenamed("member0", "vault_id");
        //risultati su HDFS in overwrite
        String outputFilePath = "hdfs://namenode:9000/output/query1/";
        sortedFilteredFailures.write().format("csv").option("header", "true").mode("overwrite").save(outputFilePath);

        //risultati su volume locale
        sortedFilteredFailures.write().format("csv").option("header", "true").mode("overwrite").save("file:///home/results/output1/");
        java.util.List<Row> rows = sortedFilteredFailures.collectAsList();


        //Redis
        try (Jedis jedis = new Jedis("redis-cache", 6379)) { // Modifica l'host e la porta se necessario
            for (Row row : rows) {

                //estrarre valori da row
                String date = row.getString(row.fieldIndex("data"));
                int vaultId = row.getInt(row.fieldIndex("vault_id"));
                String failuresCountString = String.valueOf(row.getLong(row.fieldIndex("failures_count")));

                //mappa con valori da scrivere su Redis
                Map<String, String> hash = new HashMap<>();
                hash.put("date", date);
                hash.put("vaultId", String.valueOf(vaultId));
                hash.put("failuresCount", failuresCountString);

                //chiave unica per ogni riga
                String redisKey = "query1:" + date + ":" + vaultId;

                //valori in Redis tramite hash
                jedis.hset(redisKey, hash);
            }
            System.out.println("Dati scritti su Redis con successo");
        } catch (Exception e) {
            e.printStackTrace();
        }


        spark.stop();
    }
}
