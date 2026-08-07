package fr.insee.vtl.engine.semantics.udo;

import java.util.Objects;

/** Formal parameter of a {@link UdoDefinition}. */
public final class UdoParameter {

  private final String name;
  private final Class<?> type;
  private final Object defaultValue;
  private final boolean optional;

  private UdoParameter(String name, Class<?> type, Object defaultValue, boolean optional) {
    this.name = Objects.requireNonNull(name);
    this.type = Objects.requireNonNull(type);
    this.defaultValue = defaultValue;
    this.optional = optional;
  }

  /** Parameter with a {@code default} clause (optional at call site). */
  public static UdoParameter withDefault(String name, Class<?> type, Object defaultValue) {
    return new UdoParameter(name, type, defaultValue, true);
  }

  public static UdoParameter mandatory(String name, Class<?> type) {
    return new UdoParameter(name, type, null, false);
  }

  public String getName() {
    return name;
  }

  public Class<?> getType() {
    return type;
  }

  public Object getDefaultValue() {
    return defaultValue;
  }

  public boolean isOptional() {
    return optional;
  }
}
