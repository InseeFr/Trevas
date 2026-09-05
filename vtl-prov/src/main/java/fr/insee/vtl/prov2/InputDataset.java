package fr.insee.vtl.prov2;

import java.util.List;
import java.util.Map;

/**
 * Binding declared by a fixture {@code $input} directive (one-liner or table form).
 *
 * <p>{@code rows} is empty for structure-only inputs; non-empty for data-dependent operators such as
 * {@code pivot}.
 */
public record InputDataset(String name, List<Column> columns, List<List<String>> rows) {

  public InputDataset(String name, List<Column> columns) {
    this(name, columns, List.of());
  }

  public record Column(String name, String type, String role, Map<String, String> attrs) {}
}
