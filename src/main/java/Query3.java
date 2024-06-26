import Utils.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class Query3 {

    public static void main(String[] args) {

        Utils.preProcessData(Utils.FILE_Q3,Utils.PRE_PROCESS_TEMPLATE3);
        Utils.preProcessData(Utils.PARQUET_FILE_Q3, Utils.PARQUET_TEMPLATE3);
        // Inizializza SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("Query3")
                .getOrCreate();

        //inizio query post preprocessamento
        long start = System.currentTimeMillis();

        Dataset<Row> dataframe = spark
                .read()
                .option("header", "true") // Imposta il flag header a true se il CSV ha una riga di intestazione
                //.csv("hdfs://namenode:9000/home/dataset-batch/Query3.csv"); // Modifica il percorso in base al tuo file CSV
                .parquet("hdfs://namenode:9000/home/dataset-batch/Query3.parquet");


        dataframe = dataframe.select(
                col("data"),
                col("serial_number.member1").as("serial_number"),
                col("failure.member0").as("failure"),
                col("s9_power_on_hours.member0").as("s9_power_on_hours")
        );




        //ottenere solo i serial_number che hanno failure uguale a 1
        Dataset<Row> serialNumbersWithFailures = dataframe.filter(col("failure").equalTo(1))
                .select("serial_number")
                .distinct();



        //convertire la colonna "date" in formato data
        dataframe = dataframe.withColumn("data", functions.to_timestamp(dataframe.col("data"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"));

        //trovare la data più recente per ogni "serial_number"
        Dataset<Row> maxDates = dataframe.groupBy("serial_number")
                .agg(functions.max("data").alias("max_date"));

        //unire i risultati con il DataFrame originale
        Dataset<Row> latestData = dataframe.join(maxDates, dataframe.col("serial_number").equalTo(maxDates.col("serial_number"))
                        .and(dataframe.col("data").equalTo(maxDates.col("max_date"))))
                .drop(maxDates.col("serial_number")).drop(maxDates.col("max_date"));

        //convertire la colonna "s9_power_on_hours" in formato numerico
        latestData = latestData.withColumn("s9_power_on_hours", latestData.col("s9_power_on_hours").cast("double"));

        //rename colonna serial_number in serial_number_latestData per permetterne utilizzo
        Dataset<Row> latestDataRenamed = latestData.withColumnRenamed("serial_number", "serial_number_latestData");

        //unire DataFrame utilizzando una left join
        Dataset<Row> updatedLatestData = latestDataRenamed.join(serialNumbersWithFailures, latestDataRenamed.col("serial_number_latestData").equalTo(serialNumbersWithFailures.col("serial_number")), "left_outer")
                .withColumn("failure", functions.when(col("serial_number").isNull(), col("failure")).otherwise(1))
                .drop(serialNumbersWithFailures.col("serial_number"));

        updatedLatestData.show();

        //calcolare statistiche per serial_number che hanno subito fallimenti
        Dataset<Row> failureStats = calculateStatistics(updatedLatestData.filter("failure = 1"), "s9_power_on_hours");

        failureStats = failureStats.withColumn("failure", functions.lit(1));
        //calcolare statistiche per gli hard disk che non hanno subito fallimenti
        Dataset<Row> noFailureStats = calculateStatistics(updatedLatestData.filter("failure = 0"), "s9_power_on_hours");
        noFailureStats = noFailureStats.withColumn("failure", functions.lit(0));
        //fine query
        System.out.println("Query 3 elapsed time: " + (System.currentTimeMillis()-start) + "ms\n");

        // Scrive le statistiche su HDFS
        String outputFilePath = "hdfs://namenode:9000/output/query3/";


        Dataset<Row> union = failureStats.union(noFailureStats);

        union.show();
        union.coalesce(1).write().format("csv").option("header", "true").mode("overwrite").save(outputFilePath);
        union.coalesce(1).write().format("csv").option("header", "true").mode("overwrite").save("file:///home/results/output3/");

        java.util.List<Row> rows = failureStats.collectAsList();
        java.util.List<Row> rows2 = noFailureStats.collectAsList();

        //scrivi righe in Redis
        try (Jedis jedis = new Jedis("redis-cache", 6379)) {
            for (Row row : rows) {
                //estrazione
                String min = String.valueOf(row.getDouble(row.fieldIndex("min")));
                String percentile25th = String.valueOf(row.getDouble(row.fieldIndex("25th_percentile")));
                String percentile50th = String.valueOf(row.getDouble(row.fieldIndex("50th_percentile")));
                String percentile75th = String.valueOf(row.getDouble(row.fieldIndex("75th_percentile")));
                String max = String.valueOf(row.getDouble(row.fieldIndex("max")));
                String count = String.valueOf(row.getLong(row.fieldIndex("count")));

                String failure = String.valueOf(row.getInt(row.fieldIndex("failure")));

                //creazione mappa con i valori da scrivere su Redis
                Map<String, String> hash = new HashMap<>();
                hash.put("min", min);
                hash.put("25th_percentile", percentile25th);
                hash.put("50th_percentile", percentile50th);
                hash.put("75th_percentile", percentile75th);
                hash.put("max", max);
                hash.put("count", count);
                hash.put("# failure", failure);

                jedis.hset("failureStats", hash);
            }

            for (Row row : rows2) {
                //estrarre i valori dalla riga
                String min = String.valueOf(row.getDouble(row.fieldIndex("min")));
                String percentile25th = String.valueOf(row.getDouble(row.fieldIndex("25th_percentile")));
                String percentile50th = String.valueOf(row.getDouble(row.fieldIndex("50th_percentile")));
                String percentile75th = String.valueOf(row.getDouble(row.fieldIndex("75th_percentile")));
                String max = String.valueOf(row.getDouble(row.fieldIndex("max")));
                String count = String.valueOf(row.getLong(row.fieldIndex("count")));

                String failure = String.valueOf(row.getInt(row.fieldIndex("failure")));

                Map<String, String> hash = new HashMap<>();
                hash.put("min", min);
                hash.put("25th_percentile", percentile25th);
                hash.put("50th_percentile", percentile50th);
                hash.put("75th_percentile", percentile75th);
                hash.put("max", max);
                hash.put("count", count);
                hash.put("# failure", failure);

                jedis.hset("nofailureStats", hash);
            }
            System.out.println("Dati scritti su Redis con successo");
        } catch (Exception e) {
            e.printStackTrace();
        }

        spark.stop();
    }

    //nuova versione, significativamente più veloce!
    private static Dataset<Row> calculateStatistics(Dataset<Row> data, String column) {
        return data.agg(
                min(column).alias("min"),
                expr("percentile_approx(" + column + ", 0.25)").alias("25th_percentile"),
                expr("percentile_approx(" + column + ", 0.50)").alias("50th_percentile"),
                expr("percentile_approx(" + column + ", 0.75)").alias("75th_percentile"),
                max(column).alias("max"),
                count(column).alias("count")
        );
    }
}

