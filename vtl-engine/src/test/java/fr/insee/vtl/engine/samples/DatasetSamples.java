package fr.insee.vtl.engine.samples;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;
import java.util.Arrays;
import java.util.List;

public class DatasetSamples {

  public static InMemoryDataset ds1 =
      new InMemoryDataset(
          List.of(
              new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER),
              new Structured.Component("long1", Long.class, Dataset.Role.MEASURE),
              new Structured.Component("long2", Long.class, Dataset.Role.MEASURE),
              new Structured.Component("double1", Double.class, Dataset.Role.MEASURE),
              new Structured.Component("double2", Double.class, Dataset.Role.MEASURE),
              new Structured.Component("bool1", Boolean.class, Dataset.Role.MEASURE),
              new Structured.Component("bool2", Boolean.class, Dataset.Role.MEASURE),
              new Structured.Component("string1", String.class, Dataset.Role.MEASURE),
              new Structured.Component("string2", String.class, Dataset.Role.MEASURE)),
          Arrays.asList("Toto", 30L, 300L, 12.2D, 1.22D, true, false, "toto", "t"),
          Arrays.asList("Hadrien", 10L, 1L, 1.1D, 10.11D, true, true, "hadrien", "k"),
          Arrays.asList("Nico", 20L, 250L, 12.2D, 21.1D, false, true, "nico", "l"),
          Arrays.asList("Franck", 100L, 2L, 1.21D, 100.9D, false, false, "franck", "c"));

  public static InMemoryDataset ds2 =
      new InMemoryDataset(
          List.of(
              new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER),
              new Structured.Component("long1", Long.class, Dataset.Role.MEASURE),
              new Structured.Component("double1", Double.class, Dataset.Role.MEASURE),
              new Structured.Component("bool1", Boolean.class, Dataset.Role.MEASURE),
              new Structured.Component("string1", String.class, Dataset.Role.MEASURE)),
          Arrays.asList("Hadrien", 150L, 1.1D, true, "hadrien"),
          Arrays.asList("Nico", 20L, 2.2D, true, "nico"),
          Arrays.asList("Franck", 100L, -1.21D, false, "franck"));
}
