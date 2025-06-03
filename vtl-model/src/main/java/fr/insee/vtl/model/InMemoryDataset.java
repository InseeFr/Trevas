package fr.insee.vtl.model;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Basic implementation of <code>Dataset</code> as an in-memory dataset. */
public class InMemoryDataset implements Dataset {

  private final List<DataPoint> data;
  private final DataStructure structure;

  public InMemoryDataset(List<DataPoint> data, Map<String, Component> structure) {
    this.structure = new DataStructure(structure.values());
    this.data = data;
  }

  /**
   * Constructor taking initial data and structure components types, roles and nullables.
   *
   * @param data The initial data as a list of mappings between column names and column contents.
   * @param types A mapping between the names and types of the columns as structure components.
   * @param roles A mapping between the names and roles of the columns as structure components.
   */
  public InMemoryDataset(
      List<Map<String, Object>> data, Map<String, Class<?>> types, Map<String, Role> roles) {
    if (!Objects.requireNonNull(types).keySet().equals(Objects.requireNonNull(roles).keySet())) {
      throw new IllegalArgumentException("types and role keys differ");
    }
    this.structure = new DataStructure(types, roles);
    this.data = convert(data);
  }

  /**
   * Constructor taking initial data and structure components types, roles and nullables.
   *
   * @param data The initial data as a list of mappings between column names and column contents.
   * @param types A mapping between the names and types of the columns as structure components.
   * @param roles A mapping between the names and roles of the columns as structure components.
   * @param nullables A mapping between the names and nullables of the columns as structure
   *     components.
   */
  public InMemoryDataset(
      List<Map<String, Object>> data,
      Map<String, Class<?>> types,
      Map<String, Role> roles,
      Map<String, Boolean> nullables) {
    if (!Objects.requireNonNull(types).keySet().equals(Objects.requireNonNull(roles).keySet())) {
      throw new IllegalArgumentException("types and role keys differ");
    }
    this.structure = new DataStructure(types, roles, nullables);
    this.data = convert(data);
  }

  public InMemoryDataset(DataStructure structures, List<Object>... data) {
    this.structure = structures;
    this.data = convertList(List.of(data));
  }

  /**
   * Constructor taking initial data and structure components types and roles.
   *
   * @param dataPoints The initial data as a list of list of objects representing data contents.
   * @param structure The list of structure components forming the structure of the dataset.
   */
  public InMemoryDataset(List<Component> structure, List<Object>... dataPoints) {
    this(List.of(dataPoints), structure);
  }

  /**
   * Constructor taking initial data and a list of structure components.
   *
   * @param data The initial data as a list of list of objects representing data contents.
   * @param structures The list of structure components forming the structure of the dataset.
   */
  public InMemoryDataset(List<List<Object>> data, List<Component> structures) {
    this.structure = new DataStructure(structures);
    this.data = convertList(data);
  }

  /**
   * Constructor taking initial data and a data structure.
   *
   * @param data The initial data as a list of list of objects representing data contents.
   * @param structures A <code>DataStructure</code> giving the list of structure components forming
   *     the structure of the dataset.
   */
  public InMemoryDataset(List<List<Object>> data, DataStructure structures) {
    this.structure = structures;
    this.data = convertList(data);
  }

  private List<DataPoint> convert(List<Map<String, Object>> data) {
    return Objects.requireNonNull(data).stream()
        .map(map -> new DataPoint(this.structure, map))
        .collect(Collectors.toList());
  }

  private List<DataPoint> convertList(List<List<Object>> data) {
    return Objects.requireNonNull(data).stream()
        .map(map -> new DataPoint(this.structure, map))
        .collect(Collectors.toList());
  }

  @Override
  public List<DataPoint> getDataPoints() {
    return data;
  }

  @Override
  public DataStructure getDataStructure() {
    return structure;
  }
}
