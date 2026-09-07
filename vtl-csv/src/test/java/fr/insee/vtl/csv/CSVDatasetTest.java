package fr.insee.vtl.csv;

import static fr.insee.vtl.model.Structured.DataStructure;
import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.supercsv.prefs.CsvPreference;
import org.threeten.extra.Interval;

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

  @Test
  void loadsTimePeriodAsLexicalStringAndDateAsInstant() throws IOException {
    var structure =
        new DataStructure(
            List.of(
                new Structured.Component("Id_1", Interval.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("Me_1", Instant.class, Dataset.Role.MEASURE),
                new Structured.Component("Me_2", Long.class, Dataset.Role.MEASURE)));
    Dataset dataset =
        new CSVDataset(
            structure,
            new StringReader("Id_1,Me_1,Me_2\n2010,2019-01-01,2.0\n"),
            CsvPreference.STANDARD_PREFERENCE);

    var row = dataset.getDataAsMap().get(0);
    assertThat(row.get("Id_1")).isEqualTo("2010");
    assertThat(row.get("Me_1")).isEqualTo(Instant.parse("2019-01-01T00:00:00Z"));
    assertThat(row.get("Me_2")).isEqualTo(2L);
  }
}
