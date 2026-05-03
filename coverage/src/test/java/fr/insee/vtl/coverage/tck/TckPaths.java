package fr.insee.vtl.coverage.tck;

/**
 * Display paths for flat {@link org.junit.jupiter.api.DynamicTest} names (readable in CI reports).
 */
public final class TckPaths {

  /** Separator between folder segments (single unicode char avoids clash with path chars). */
  public static final String SEGMENT_SEP = " \u00BB ";

  private TckPaths() {}

  /**
   * Builds a hierarchical label: {@code "" + "A"} → {@code "A"}, {@code "A" + "B"} → {@code "A »
   * B"}.
   */
  public static String join(String prefix, String segment) {
    String seg = segment == null ? "" : segment;
    if (seg.isEmpty()) {
      return prefix == null ? "" : prefix;
    }
    if (prefix == null || prefix.isEmpty()) {
      return seg;
    }
    return prefix + SEGMENT_SEP + seg;
  }
}
