package fr.insee.vtl.coverage.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import fr.insee.vtl.csv.CSVDataset;
import fr.insee.vtl.jackson.TrevasModule;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;
import org.supercsv.prefs.CsvPreference;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JSONStructureLoader {

    public static Map<String, List<Structured.Component>> loadStructures(File jsonFile) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new TrevasModule());

        String json = new String(Files.readAllBytes(jsonFile.toPath()), StandardCharsets.UTF_8);
        JsonNode root = mapper.readTree(json);

        Map<String, List<Structured.Component>> structureMap = new HashMap<>();

        JsonNode structures = root.get("structures");
        if (structures != null && structures.isArray()) {
            for (JsonNode structure : structures) {
                String structureName = structure.get("name").asText();
                List<Structured.Component> components = new ArrayList<>();

                JsonNode componentArray = structure.get("components");
                if (componentArray != null && componentArray.isArray()) {
                    for (JsonNode componentNode : componentArray) {
                        String name = componentNode.get("name").asText();
                        String role = componentNode.get("role").asText().toUpperCase();
                        String type = componentNode.get("data_type").asText().toUpperCase();

                        // Build the JSON string dynamically
                        String componentJson = String.format(
                                "{ \"name\": \"%s\", \"type\": \"%s\", \"role\": \"%s\" }",
                                name, type, role
                        );

                        Structured.Component component = mapper.readValue(componentJson, Structured.Component.class);
                        components.add(component);
                    }
                }

                structureMap.put(structureName, components);
            }
        }
        return structureMap;
    }


    public static Map<String, Dataset> loadDatasetsFromCSV(File dataStructureFile) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new TrevasModule());

        String json = new String(Files.readAllBytes(dataStructureFile.toPath()), StandardCharsets.UTF_8);
        JsonNode root = mapper.readTree(json);

        File parentDir = dataStructureFile.getParentFile();

        Map<String, List<Structured.Component>> structures = loadStructures(dataStructureFile);
        Map<String, Dataset> datasets = new HashMap<>();

        JsonNode datasetArray = root.get("datasets");
        if (datasetArray != null && datasetArray.isArray()) {
            for (JsonNode datasetNode : datasetArray) {
                String datasetName = datasetNode.get("name").asText(); // ex: "DS_1"
                String structureRef = datasetNode.get("structure").asText(); // ex: "DS_1"

                List<Structured.Component> components = structures.get(structureRef);
                if (components == null) continue;
                Structured.DataStructure structure = new Structured.DataStructure(components);

                File csvFile = new File(parentDir, datasetName + ".csv");

                if (!csvFile.exists()) {
                    throw new FileNotFoundException("Missing CSV file for dataset: " + datasetName);
                }

                Dataset dataset = new CSVDataset(structure, new FileReader(csvFile), CsvPreference.STANDARD_PREFERENCE);
                datasets.put(datasetName, dataset);
            }
        }
        return datasets;
    }

}