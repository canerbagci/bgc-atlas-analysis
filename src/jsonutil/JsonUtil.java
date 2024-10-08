package jsonutil;

import javax.json.JsonObject;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class JsonUtil {

    public static String getStringOrDefault(JsonObject obj, String key) {
        return getStringOrDefault(obj, key, "");
    }

    public static String getStringOrDefault(JsonObject obj, String key, String defaultVal) {
        return obj.isNull(key) ? defaultVal : obj.getString(key);
    }

    public static void writeJSON2File(JsonObject obj, String out) {
        try(BufferedWriter bw = new BufferedWriter(new FileWriter(new File(out)))) {
            bw.write(obj.toString());
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public static Double getDoubleOrDefault(JsonObject obj, String key, double defaultVal) {
        return obj.isNull(key) ? defaultVal : obj.getJsonNumber(key).doubleValue();
    }
}
