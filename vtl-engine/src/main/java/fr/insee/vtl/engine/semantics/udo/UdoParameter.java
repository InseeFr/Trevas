package fr.insee.vtl.engine.semantics.udo;

import fr.insee.vtl.model.Structured;
import java.util.Objects;

/** Formal parameter of a {@link UdoDefinition}. */
public final class UdoParameter {

  private final String name;
  private final Class<?> type;
  private final Structured.DataStructure datasetStructure;
  private final Object defaultValue;
  private final boolean optional;

  private UdoParameter(
      String name,
      Class<?> type,
      Structured.DataStructure datasetStructure,
      Object defaultValue,
      boolean optional) {
    this.name = Objects.requireNonNull(name);
    this.type = Objects.requireNonNull(type);
    this.datasetStructure = datasetStructure;
    this.defaultValue = defaultValue;
    this.optional = optional;
  }

  /** Parameter with a {@code default} clause (optional at call site). */
  public static UdoParameter withDefault(String name, Class<?> type, Object defaultValue) {
    return withDefault(name, type, null, defaultValue);
  }

  public static UdoParameter withDefault(
      String name, Class<?> type, Structured.DataStructure datasetStructure, Object defaultValue) {
    return new UdoParameter(name, type, datasetStructure, defaultValue, true);
  }

  public static UdoParameter mandatory(String name, Class<?> type) {
    return mandatory(name, type, null);
  }

  public static UdoParameter mandatory(
      String name, Class<?> type, Structured.DataStructure datasetStructure) {
    return new UdoParameter(name, type, datasetStructure, null, false);
  }

  public String getName() {
    return name;
  }

  public Class<?> getType() {
    return type;
  }

  public Structured.DataStructure getDatasetStructure() {
    return datasetStructure;
  }

  public Object getDefaultValue() {
    return defaultValue;
  }

  public boolean isOptional() {
    return optional;
  }
}
