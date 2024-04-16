package fr.insee.vtl.jackson;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.utils.Java8Helpers;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class DatasetDeserializerTest extends AbstractMapperTest {

    @Test
    public void testDeserializeDataset() throws IOException {
        InputStream jsonStream = getClass().getResourceAsStream("/dataset.json");

        Dataset dataset = mapper.readValue(jsonStream, Dataset.class);

        assertThat(dataset.getDataStructure().values()).containsExactly(
                new Dataset.Component("CONTINENT", String.class, Dataset.Role.IDENTIFIER, false),
                new Dataset.Component("COUNTRY", String.class, Dataset.Role.IDENTIFIER, false),
                new Dataset.Component("POP", Long.class, Dataset.Role.MEASURE, false),
                new Dataset.Component("AREA", Double.class, Dataset.Role.MEASURE, true)
        );

        List<Object> nulls = new ArrayList<>();
        nulls.add(null);
        nulls.add(null);
        nulls.add(null);
        nulls.add(null);
        assertThat(dataset.getDataAsList()).containsExactly(
                Java8Helpers.listOf("Europe", "France", 67063703L, 643.801),
                Java8Helpers.listOf("Europe", "Norway", 5372191L, 385.203),
                Java8Helpers.listOf("Oceania", "New Zealand", 4917000L, 268.021),
                nulls
        );

    }

    @Test
    public void testDeserializeInvertedDataset() throws IOException {
        InputStream jsonStream = getClass().getResourceAsStream("/dataset-inverted.json");

        Dataset dataset = mapper.readValue(jsonStream, Dataset.class);

        assertThat(dataset.getDataStructure().values()).containsExactly(
                new Dataset.Component("CONTINENT", String.class, Dataset.Role.IDENTIFIER, false),
                new Dataset.Component("COUNTRY", String.class, Dataset.Role.IDENTIFIER, false),
                new Dataset.Component("POP", Long.class, Dataset.Role.MEASURE, true),
                new Dataset.Component("AREA", Double.class, Dataset.Role.MEASURE, true)
        );

        List<Object> nulls = new ArrayList<>();
        nulls.add(null);
        nulls.add(null);
        nulls.add(null);
        nulls.add(null);
        assertThat(dataset.getDataAsList()).containsExactly(
                Java8Helpers.listOf("Europe", "France", 67063703L, 643.801),
                Java8Helpers.listOf("Europe", "Norway", 5372191L, 385.203),
                Java8Helpers.listOf("Oceania", "New Zealand", 4917000L, 268.021),
                nulls
        );

    }
}