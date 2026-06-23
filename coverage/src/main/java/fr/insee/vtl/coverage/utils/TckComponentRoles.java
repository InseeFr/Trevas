package fr.insee.vtl.coverage.utils;

/** Maps TCK / SDMX role labels to Trevas {@link fr.insee.vtl.model.Dataset.Role} names. */
public final class TckComponentRoles {

  private TckComponentRoles() {}

  /**
   * Normalizes a role from TCK structure JSON to a name accepted by {@link
   * fr.insee.vtl.model.Dataset.Role#fromName(String)}.
   */
  public static String toTrevasRole(String tckRole) {
    String normalized = tckRole.trim().replaceAll("\\s+", "").toUpperCase();
    return switch (normalized) {
      case "DIMENSION", "COMPONENT" -> "IDENTIFIER";
      case "VIRAL_ATTRIBUTE" -> "VIRALATTRIBUTE";
      default -> normalized;
    };
  }
}
