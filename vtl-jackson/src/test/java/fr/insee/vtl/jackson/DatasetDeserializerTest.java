package fr.insee.vtl.jackson;

import fr.insee.vtl.model.Dataset;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class DatasetDeserializerTest extends AbstractMapperTest {

    @Test
    public void testDeserializeDataset() throws IOException {
        var jsonStream = getClass().getResourceAsStream("/dataset.json");

        var dataset = mapper.readValue(jsonStream, Dataset.class);

        assertThat(dataset.getDataStructure()).containsExactly(
                new Dataset.Component("CONTINENT", String.class, Dataset.Role.IDENTIFIER),
                new Dataset.Component("COUNTRY", String.class, Dataset.Role.IDENTIFIER),
                new Dataset.Component("POP", Long.class, Dataset.Role.MEASURE),
                new Dataset.Component("AREA", Double.class, Dataset.Role.MEASURE)
        );

        List<Object> nulls = new ArrayList<>();
        nulls.add(null);
        nulls.add(null);
        nulls.add(null);
        nulls.add(null);
        assertThat(dataset.getDataPoints()).containsExactly(
                List.of("Europe", "France", 67063703L, 643.801),
                List.of("Europe", "Norway", 5372191L, 385.203),
                List.of("Oceania", "New Zealand", 4917000L, 268.021),
                nulls
        );

    }
}