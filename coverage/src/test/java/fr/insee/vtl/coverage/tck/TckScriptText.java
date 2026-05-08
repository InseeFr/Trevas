package fr.insee.vtl.coverage.tck;

/** Shared formatting for VTL script text in TCK outputs. */
public final class TckScriptText {

  private TckScriptText() {}

  public static String summary(String script, int maxChars) {
    String normalized = normalizeScript(script);
    if (normalized == null || normalized.isBlank()) {
      return "(empty)";
    }
    String oneLine =
        normalized.replace('\n', ' ').replace('\r', ' ').trim().replaceAll("\\s+", " ");
    return oneLine.length() > maxChars ? oneLine.substring(0, maxChars) + "…" : oneLine;
  }

  public static void appendFull(StringBuilder sb, String script, int maxChars) {
    sb.append("VTL script: ").append(summary(script, 120)).append(System.lineSeparator());
    sb.append("--- transformation.vtl ---").append(System.lineSeparator());
    String normalized = normalizeScript(script);
    if (normalized == null) {
      sb.append("(null)");
      return;
    }
    String truncated =
        normalized.length() > maxChars
            ? normalized.substring(0, maxChars) + "\n… (truncated)"
            : normalized;
    sb.append(truncated);
  }

  private static String normalizeScript(String script) {
    if (script == null) {
      return null;
    }
    // TCK payload may contain XML/HTML entities (e.g. &quot;) in script text.
    return script
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&");
  }
}
