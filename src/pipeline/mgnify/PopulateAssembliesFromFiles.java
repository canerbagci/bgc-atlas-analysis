package pipeline.mgnify;

import data.mgnify.Assembly;
import dbutil.Database;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class PopulateAssembliesFromFiles {

    static Database database = new Database();

    public static void main(String[] args) {
        String assembliesDir = args[0];
        List<Assembly> assemblies = new ArrayList<>();

        File[] files = new File(assembliesDir).listFiles();
        for(File f : files) {
            Assembly assembly = new Assembly(f);
            assemblies.add(assembly);
            System.out.println(assemblies.size());
        }

        System.out.println(assemblies.size());

        database.insertAssemblies(assemblies);

    }
}
