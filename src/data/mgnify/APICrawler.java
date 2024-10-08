package data.mgnify;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class APICrawler {

    public static JsonObject getPageData(int page, int pageSize) {

        System.out.println("Reading page: " + page);
        String apiBase = "https://www.ebi.ac.uk/metagenomics/api/v1/analyses?page=" + page + "&page_size=" + pageSize + "&format=json";
        return getJsonObjectFromURL(apiBase);
    }


    public static JsonObject getPagination(int pageSize) {
        String apiBase = "https://www.ebi.ac.uk/metagenomics/api/v1/analyses?page=1&page_size=" + pageSize + "&format=json";
        try(InputStream is = new URL(apiBase).openStream();
            JsonReader reader = Json.createReader(new InputStreamReader(is, StandardCharsets.UTF_8))){
            JsonObject firstPage = reader.readObject();
            JsonObject meta = firstPage.getJsonObject("meta");
            JsonObject pagination = meta.getJsonObject("pagination");
            return pagination;
        }catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return null;
    }

    public static JsonObject getJsonObjectFromURL(String url) {
        try(InputStream is = new URL(url).openStream();
            JsonReader reader = Json.createReader(new InputStreamReader(is, StandardCharsets.UTF_8))){
            JsonObject pageObj = reader.readObject();
            return pageObj;
        }catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return null;
    }

    public static void downloadFile(String url, String filePath) {
        try (BufferedInputStream in = new BufferedInputStream(new URL(url).openStream());
             FileOutputStream fileOutputStream = new FileOutputStream(filePath)) {
            byte[] dataBuffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -1) {
                fileOutputStream.write(dataBuffer, 0, bytesRead);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
