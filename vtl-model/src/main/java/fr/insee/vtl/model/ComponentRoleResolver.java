package fr.insee.vtl.model;

/** Resolves VTL component role names from metadata, JSON, and other string sources. */
public final class ComponentRoleResolver {

  private ComponentRoleResolver() {}

  /**
   * Parses a role name (JSON {@code role}, Spark {@code vtlRole}, etc.).
   *
   * <p>Accepts enum names and common aliases ({@code viral attribute}, {@code DIMENSION} → {@link
   * Dataset.Role#IDENTIFIER}).
   */
  public static Dataset.Role parseRoleName(String roleName) {
    if (roleName == null || roleName.isBlank()) {
      throw new IllegalArgumentException("role name is blank");
    }
    String normalized = roleName.trim().replaceAll("\\s+", "").toUpperCase();
    return switch (normalized) {
      case "VIRALATTRIBUTE", "VIRAL_ATTRIBUTE" -> Dataset.Role.VIRALATTRIBUTE;
      case "DIMENSION", "COMPONENT" -> Dataset.Role.IDENTIFIER;
      case "IDENTIFIER", "MEASURE", "ATTRIBUTE" -> Dataset.Role.valueOf(normalized);
      default -> Dataset.Role.valueOf(normalized);
    };
  }
}
