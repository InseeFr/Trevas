package fr.insee.vtl.coverage.tck;

/** Shared formatting for VTL script text in TCK outputs. */
public final class TckScriptText {

  private TckScriptText() {}

  public static String summary(String script, int maxChars) {
    if (script == null || script.isBlank()) {
      return "(empty)";
    }
    String oneLine = script.replace('\n', ' ').replace('\r', ' ').trim().replaceAll("\\s+", " ");
    return oneLine.length() > maxChars ? oneLine.substring(0, maxChars) + "…" : oneLine;
  }

  public static void appendFull(StringBuilder sb, String script, int maxChars) {
    sb.append("VTL script: ").append(summary(script, 120)).append(System.lineSeparator());
    sb.append("--- transformation.vtl ---").append(System.lineSeparator());
    if (script == null) {
      sb.append("(null)");
      return;
    }
    String truncated =
        script.length() > maxChars ? script.substring(0, maxChars) + "\n… (truncated)" : script;
    sb.append(truncated);
  }
}
