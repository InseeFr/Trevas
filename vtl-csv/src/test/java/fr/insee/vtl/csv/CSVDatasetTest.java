package fr.insee.vtl.csv;

import static fr.insee.vtl.model.Structured.DataStructure;
import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class CSVDatasetTest {
  @Test
  void createCSVDataset() throws IOException {
    List<Structured.Component> components = new ArrayList<>();
    components.add(new Structured.Component("REF_AREA", String.class, Dataset.Role.IDENTIFIER));
    components.add(new Structured.Component("TIME_PERIOD", String.class, Dataset.Role.IDENTIFIER));
    components.add(new Structured.Component("NB_COM", String.class, Dataset.Role.MEASURE));
    components.add(new Structured.Component("POP_MUNI", String.class, Dataset.Role.MEASURE));
    components.add(new Structured.Component("POP_TOT", String.class, Dataset.Role.MEASURE));
    DataStructure structure = new DataStructure(components);

    Dataset dataset =
        new CSVDataset(structure, new FileReader("src/test/resources/LEGAL_POP_NUTS3.csv"));

    assertThat(dataset.getDataPoints().size()).isEqualTo(100);
  }
}
