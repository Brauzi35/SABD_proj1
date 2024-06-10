package Nifi;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class NifiREST {

    public static String nifiPOST(String templateFile, String stringUrl) {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            System.out.println("POST " + stringUrl + " file=" + templateFile);
            File file = new File(templateFile);

            HttpPost uploadFile = new HttpPost(stringUrl);
            MultipartEntityBuilder builder = MultipartEntityBuilder.create();
            builder.addPart("template", new FileBody(file, ContentType.DEFAULT_BINARY));

            HttpEntity multipart = builder.build();
            uploadFile.setEntity(multipart);

            HttpResponse response = httpClient.execute(uploadFile);
            HttpEntity responseEntity = response.getEntity();

            if (responseEntity != null) {
                return EntityUtils.toString(responseEntity);
            } else {
                return null;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static boolean deleteNifi(String stringUrl, boolean withJson) {
        HttpURLConnection conn = null;
        try {
            System.out.println("DELETE " + stringUrl);
            URL url = new URL(stringUrl);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("DELETE");
            if (withJson) {
                conn.setRequestProperty("Content-Type", "application/json");
            }

            int code = conn.getResponseCode();
            return code == HttpURLConnection.HTTP_OK; // 200 OK
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    public static JSONObject nifiGET(String stringUrl) {
        HttpURLConnection conn = null;
        try {
            System.out.println("GET " + stringUrl);
            URL url = new URL(stringUrl);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");

            try (InputStream is = conn.getInputStream()) {
                String response = IOUtils.toString(is, StandardCharsets.UTF_8);
                return new JSONObject(response);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    public static boolean nifiPUT(String stringUrl, String body) {
        HttpURLConnection conn = null;
        try {
            URL url = new URL(stringUrl);
            conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("PUT");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Accept", "application/json");

            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = body.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            int status = conn.getResponseCode();
            System.out.println("Status PUT: " + status + " for: " + body);
            return status == HttpURLConnection.HTTP_OK; // 200 OK
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    public static JSONObject postRequest(String stringUrl, String json) {
        HttpURLConnection conn = null;
        try {
            URL url = new URL(stringUrl);
            conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");

            if (json != null) {
                try (OutputStream os = conn.getOutputStream()) {
                    byte[] input = json.getBytes(StandardCharsets.UTF_8);
                    os.write(input, 0, input.length);
                }
            }

            StringBuilder response = new StringBuilder();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                String responseLine;
                while ((responseLine = br.readLine()) != null) {
                    response.append(responseLine.trim());
                }
            }

            return new JSONObject(response.toString());
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

}
