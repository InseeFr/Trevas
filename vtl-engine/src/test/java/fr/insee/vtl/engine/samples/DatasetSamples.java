package fr.insee.vtl.engine.samples;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;

import java.util.Arrays;
import java.util.List;

public class DatasetSamples {

    public static InMemoryDataset ds1 = new InMemoryDataset(
            List.of(
                    new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("long1", Long.class, Dataset.Role.MEASURE),
                    new Structured.Component("long2", Long.class, Dataset.Role.MEASURE),
                    new Structured.Component("double1", Double.class, Dataset.Role.MEASURE),
                    new Structured.Component("double2", Double.class, Dataset.Role.MEASURE),
                    new Structured.Component("bool1", Boolean.class, Dataset.Role.MEASURE),
                    new Structured.Component("bool2", Boolean.class, Dataset.Role.MEASURE),
                    new Structured.Component("string1", String.class, Dataset.Role.MEASURE),
                    new Structured.Component("string2", String.class, Dataset.Role.MEASURE)
            ),
            Arrays.asList("Toto", 30, 300, 12.2, 1.22, true, false, "toto", "t"),
            Arrays.asList("Hadrien", 10, 1, 1.1, 10.11, true, true, "hadrien", "k"),
            Arrays.asList("Nico", 20, 250, 12.2, 21.1, false, true, "nico", "l"),
            Arrays.asList("Franck", 100, 2, 1.21, 100.9, false, false, "franck", "c")
    );

    public static InMemoryDataset ds2 = new InMemoryDataset(
            List.of(
                    new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("long1", Long.class, Dataset.Role.MEASURE),
                    new Structured.Component("double1", Double.class, Dataset.Role.MEASURE),
                    new Structured.Component("bool1", Boolean.class, Dataset.Role.MEASURE),
                    new Structured.Component("string1", String.class, Dataset.Role.MEASURE)
            ),
            Arrays.asList("Hadrien", 150, 1.1, true, "hadrien"),
            Arrays.asList("Nico", 20, 2.2, true, "nico"),
            Arrays.asList("Franck", 100, -1.21, false, "franck")
    );
}
