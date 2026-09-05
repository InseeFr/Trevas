package fr.insee.vtl.prov2;

import java.util.List;
import java.util.Map;

/** Binding declared by a fixture {@code $input} directive (one-liner form in PR-2). */
public record InputDataset(String name, List<Column> columns) {

  public record Column(String name, String type, String role, Map<String, String> attrs) {}
}
