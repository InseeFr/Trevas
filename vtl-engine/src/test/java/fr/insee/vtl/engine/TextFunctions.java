package fr.insee.vtl.engine;

import fr.insee.vtl.model.utils.Java8Helpers;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;

import java.util.Arrays;

public class TextFunctions {
    public static String testTrim(String str) {
        return str.trim();
    }

    public static String testUpper(String str) {
        return str.toUpperCase();
    }

    public static Dataset loadS3() {
        return new InMemoryDataset(
                Java8Helpers.listOf(
                        new Structured.Component("name", String.class, Dataset.Role.IDENTIFIER),
                        new Structured.Component("age", Long.class, Dataset.Role.MEASURE),
                        new Structured.Component("weight", Long.class, Dataset.Role.MEASURE)
                ),
                Arrays.asList("Toto", null, 100L),
                Arrays.asList("Hadrien", 10L, 11L),
                Arrays.asList("Nico", 11L, 10L),
                Arrays.asList("Franck", 12L, 9L)
        );
    }
}
