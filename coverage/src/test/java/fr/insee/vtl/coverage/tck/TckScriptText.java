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
    // TCK payload may contain XML/HTML entities, sometimes double-escaped
    // (e.g. &amp;quot;), so decode repeatedly until stable.
    String decoded = script;
    for (int i = 0; i < 4; i++) {
      String next = decodeXmlEntities(decoded);
      if (next.equals(decoded)) {
        break;
      }
      decoded = next;
    }
    return decoded;
  }

  private static String decodeXmlEntities(String text) {
    return text.replace("&quot;", "\"")
        .replace("&#34;", "\"")
        .replace("&apos;", "'")
        .replace("&#39;", "'")
        .replace("&lt;", "<")
        .replace("&#60;", "<")
        .replace("&gt;", ">")
        .replace("&#62;", ">")
        .replace("&amp;", "&")
        .replace("&#38;", "&");
  }
}
