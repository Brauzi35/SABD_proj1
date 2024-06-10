package Utils;

import Nifi.NifiFlow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class Utils {
    private Utils() {
    }

    public static final String NIFI_URL = "http://nifi:8181/nifi-api/";
    public static final String DOWNLOAD_TEMPLATE = "/home/Templates/downloadDatatoHDFS.xml";
    public static final String PRE_PROCESS_TEMPLATE1 = "/home/Templates/template_Q1.xml";
    public static final String PRE_PROCESS_TEMPLATE2 = "/home/Templates/template_Q2.xml";
    public static final String PRE_PROCESS_TEMPLATE3 = "/home/Templates/template_Q3.xml";

    public static final String PARQUET_TEMPLATE1 = "/home/Templates/csvToParquetQ1.xml";
    public static final String PARQUET_TEMPLATE2 = "/home/Templates/csvToParquetQ2.xml";
    public static final String PARQUET_TEMPLATE3 = "/home/Templates/csvToParquetQ3.xml";
    public static final String FILE_Q1 = "hdfs://namenode:9000/home/dataset-batch/Query1.csv";
    public static final String FILE_Q2 = "hdfs://namenode:9000/home/dataset-batch/Query2.csv";
    public static final String FILE_Q3 = "hdfs://namenode:9000/home/dataset-batch/Query3.csv";
    public static final String PARQUET_FILE_Q1 = "hdfs://namenode:9000/home/dataset-batch/Query1.parquet";
    public static final String PARQUET_FILE_Q2 = "hdfs://namenode:9000/home/dataset-batch/Query2.parquet";
    public static final String PARQUET_FILE_Q3 = "hdfs://namenode:9000/home/dataset-batch/Query3.parquet";
    public static final String DATASET_CSV = "hdfs://namenode:9000/home/download/raw_data_medium-utv_sorted.csv";

    public static List<String> extractIdsFromJsonArray(JSONArray array) {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < array.length(); i++) {
            JSONObject jsonObject = array.getJSONObject(i);
            ids.add(jsonObject.getString("id"));
        }
        return ids;
    }


    //verifico se un determinato file è presente all'interno di hdfs
    private static boolean availableHDFSfile(String file_name){

        Configuration config = new Configuration();
        config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        try {
            //Hadoop DFS Path - Input file
            FileSystem hdfs = FileSystem.get(URI.create(file_name), config);
            //controllo se l'input è valido
            if (hdfs.exists(new org.apache.hadoop.fs.Path(file_name))) {
                System.out.println(file_name + " File Present.");
                return true;
            }
            else{
                System.out.println(file_name + " not found.");
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    //faccio il download del dataset se non presente su HDFS
    public static void downloadFiletoHDFS(){

        if(!availableHDFSfile(DATASET_CSV)){
            NifiFlow templateIstance = new NifiFlow(DOWNLOAD_TEMPLATE,NIFI_URL);
            templateIstance.uploadAndInstantiateTemplate();
            templateIstance.run();

            // Attendi finché il file non è stato scaricato
            while (!availableHDFSfile(DATASET_CSV)) {
                try {
                    Thread.sleep(5000); // Attendi 5 secondi prima di controllare di nuovo
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            templateIstance.stop();
            templateIstance.remove();
            System.out.println("Dataset downloaded.");
        }else{
            System.out.println("Dataset already downloaded.");
        }
    }

    public static void preProcessData(String file_name, String templateQ1){
        downloadFiletoHDFS();
        if(!availableHDFSfile(file_name)){
            NifiFlow templateIstance = new NifiFlow(templateQ1,NIFI_URL);
            templateIstance.uploadAndInstantiateTemplate();
            templateIstance.run();

            //attendi finché il file non è stato scaricato
            while (!availableHDFSfile(file_name)) {
                try {
                    Thread.sleep(5000); // Attendi 5 secondi prima di controllare di nuovo
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            templateIstance.stop();
            templateIstance.remove();
            System.out.println("File downloaded.");
        }else{
            System.out.println("File already downloaded.");
        }

    }


}
