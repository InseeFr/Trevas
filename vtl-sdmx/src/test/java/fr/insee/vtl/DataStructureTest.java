package fr.insee.vtl;

import fr.insee.vtl.csv.CSVDataset;
import fr.insee.vtl.sdmx.TrevasSDMXUtils;
import org.junit.jupiter.api.Test;

import java.io.FileReader;
import java.io.IOException;

import static fr.insee.vtl.model.Structured.DataStructure;

public class DataStructureTest {

    // https://github.com/sdmx-twg/sdmx-ml/tree/master/samples
    @Test
    public void buildDataset() throws IOException {
        DataStructure structure = TrevasSDMXUtils.buildStructureFromSDMX3("src/test/resources/DSD_BPE_TOWN.xml", "BPE_CUBE_2021");

        CSVDataset dataset = new CSVDataset(structure, new FileReader("src/test/resources/BPE_TOWN_SAMPLE.csv"));

        dataset.getDataAsMap().forEach(System.out::println);
    }
}
