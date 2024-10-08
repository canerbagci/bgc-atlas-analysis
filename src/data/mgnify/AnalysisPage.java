package data.mgnify;

import jsonutil.JsonUtil;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class AnalysisPage {


    private final JsonObject pageData;
    private final int pageNumber;

    public AnalysisPage(JsonObject pageData, String outDir, int pageNumber) {
        this.pageData = pageData;
        this.pageNumber = pageNumber;
        JsonUtil.writeJSON2File(this.pageData, outDir + File.separator + "pages" + File.separatorChar + "page" + pageNumber + ".json");
    }

    public List<Assembly> parse(String outDir) {
        List<Assembly> assemblies = new ArrayList<>();

        JsonArray data = this.pageData.getJsonArray("data");
        for (JsonValue datum : data) {
            JsonObject attributes = datum.asJsonObject().getJsonObject("attributes");
            JsonString experimentType = attributes.getJsonString("experiment-type");
            if(!experimentType.getString().equals("assembly"))
                continue;
            Assembly assembly = createAssembly(datum.asJsonObject(), outDir);
            assemblies.add(assembly);
        }
        return assemblies;
    }

    private Assembly createAssembly(JsonObject assemblyObject, String outDir) {
        Assembly assembly = new Assembly(assemblyObject, outDir);
        return assembly;
    }

}
