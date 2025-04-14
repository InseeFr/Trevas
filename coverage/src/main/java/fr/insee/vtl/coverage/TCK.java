package fr.insee.vtl.coverage;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.insee.vtl.coverage.model.Folder;
import fr.insee.vtl.coverage.model.Test;
import fr.insee.vtl.coverage.utils.JSONStructureLoader;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;

public class TCK {
    private static final String TCK_FOLDER_PATH = "src/main/resources/Engine_files";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void runTCK() {
        try {
            List<Folder> folders = loadInput(new File(TCK_FOLDER_PATH));
            System.out.println(folders);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<Folder> loadInput(File path) throws Exception {
        List<Folder> folders = new ArrayList<>();
        File[] files = path.listFiles();
        if (files != null) {
            boolean isTestFolder = containsTestFiles(files);

            if (isTestFolder) {
                Folder folder = new Folder();
                folder.setName(path.getName());
                Test test = new Test();

                // Lecture des fichiers
                for (File file : files) {
                    switch (file.getName()) {
                        case "input.json":
                            test.setInput(JSONStructureLoader.loadDatasetsFromCSV(file));
                            break;
                        case "output.json":
                            test.setOutputs(JSONStructureLoader.loadDatasetsFromCSV(file));
                            break;
                        case "transformation.vtl":
                            String script = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
                            test.setScript(script);
                            break;
                    }
                }
                folder.setTest(test);
                folders.add(folder);
            } else {
                // Dossier intermédiaire
                for (File file : files) {
                    if (file.isDirectory()) {
                        Folder folder = new Folder();
                        folder.setName(file.getName());
                        folder.setFolders(loadInput(file));
                        folders.add(folder);
                    }
                }
            }
        }
        return folders;
    }

    private static boolean containsTestFiles(File[] files) {
        Set<String> required = new HashSet<>(Arrays.asList(
                "input.json", "output.json", "transformation.vtl"
        ));
        Set<String> found = new HashSet<>();
        for (File file : files) {
            if (required.contains(file.getName())) {
                found.add(file.getName());
            }
        }
        return found.containsAll(required);
    }
}