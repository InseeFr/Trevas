package fr.insee.vtl.engine.semantics.udo;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;
import java.util.Objects;

/** Formal parameter of a {@link UdoDefinition}. */
public final class UdoParameter {

  private final String name;
  private final Class<?> type;
  private final UdoDatasetSignature datasetSignature;
  private final Dataset.Role componentRole;
  private final Class<?> componentScalarType;
  private final UdoRulesetKind rulesetKind;
  private final Object defaultValue;
  private final boolean optional;

  private UdoParameter(
      String name,
      Class<?> type,
      UdoDatasetSignature datasetSignature,
      Dataset.Role componentRole,
      Class<?> componentScalarType,
      UdoRulesetKind rulesetKind,
      Object defaultValue,
      boolean optional) {
    this.name = Objects.requireNonNull(name);
    this.type = Objects.requireNonNull(type);
    this.datasetSignature = datasetSignature;
    this.componentRole = componentRole;
    this.componentScalarType = componentScalarType;
    this.rulesetKind = rulesetKind;
    this.defaultValue = defaultValue;
    this.optional = optional;
  }

  /** Parameter with a {@code default} clause (optional at call site). */
  public static UdoParameter withDefault(String name, Class<?> type, Object defaultValue) {
    return withDefault(name, type, null, defaultValue);
  }

  public static UdoParameter withDefault(
      String name, Class<?> type, UdoDatasetSignature datasetSignature, Object defaultValue) {
    return new UdoParameter(name, type, datasetSignature, null, null, null, defaultValue, true);
  }

  public static UdoParameter mandatory(String name, Class<?> type) {
    return mandatory(name, type, null);
  }

  public static UdoParameter mandatory(
      String name, Class<?> type, UdoDatasetSignature datasetSignature) {
    return new UdoParameter(name, type, datasetSignature, null, null, null, null, false);
  }

  public static UdoParameter mandatoryComponent(
      String name, Dataset.Role role, Class<?> scalarType) {
    return new UdoParameter(
        name, Structured.Component.class, null, role, scalarType, null, null, false);
  }

  static UdoParameter withDefaultComponent(
      String name, Dataset.Role role, Class<?> scalarType, Object defaultValue) {
    return new UdoParameter(
        name, Structured.Component.class, null, role, scalarType, null, defaultValue, true);
  }

  static UdoParameter mandatoryParsed(
      String name,
      Class<?> type,
      UdoDatasetSignature datasetSignature,
      Dataset.Role componentRole,
      Class<?> componentScalarType,
      UdoRulesetKind rulesetKind) {
    return new UdoParameter(
        name, type, datasetSignature, componentRole, componentScalarType, rulesetKind, null, false);
  }

  static UdoParameter withDefaultParsed(
      String name,
      Class<?> type,
      UdoDatasetSignature datasetSignature,
      Dataset.Role componentRole,
      Class<?> componentScalarType,
      UdoRulesetKind rulesetKind,
      Object defaultValue) {
    return new UdoParameter(
        name,
        type,
        datasetSignature,
        componentRole,
        componentScalarType,
        rulesetKind,
        defaultValue,
        true);
  }

  public String getName() {
    return name;
  }

  public Class<?> getType() {
    return type;
  }

  public UdoDatasetSignature getDatasetSignature() {
    return datasetSignature;
  }

  public boolean isComponentParam() {
    return componentRole != null;
  }

  public Dataset.Role getComponentRole() {
    return componentRole;
  }

  public Class<?> getComponentScalarType() {
    return componentScalarType;
  }

  public boolean isRulesetParam() {
    return rulesetKind != null;
  }

  public UdoRulesetKind getRulesetKind() {
    return rulesetKind;
  }

  public Object getDefaultValue() {
    return defaultValue;
  }

  public boolean isOptional() {
    return optional;
  }
}
