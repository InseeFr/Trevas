package fr.insee.vtl.prov2;

/**
 * VTL scalar type names ({@code STRING}, {@code INTEGER}, …) ↔ Java classes used when materializing
 * {@code $input} bindings for the structure oracle.
 */
final class VtlJavaTypes {

  private VtlJavaTypes() {}

  static Class<?> javaType(String vtlType) {
    return switch (vtlType) {
      case "STRING" -> String.class;
      case "INTEGER" -> Long.class;
      case "NUMBER" -> Double.class;
      case "BOOLEAN" -> Boolean.class;
      case "DATE" -> java.time.Instant.class;
      default -> throw new UnsupportedOperationException("unsupported: type " + vtlType);
    };
  }

  static Object cellValue(String raw, String vtlType) {
    return switch (vtlType) {
      case "STRING" -> raw;
      case "INTEGER" -> Long.parseLong(raw);
      case "NUMBER" -> Double.parseDouble(raw);
      case "BOOLEAN" -> Boolean.parseBoolean(raw);
      case "DATE" -> java.time.Instant.parse(raw);
      default -> throw new UnsupportedOperationException("unsupported: type " + vtlType);
    };
  }
}
