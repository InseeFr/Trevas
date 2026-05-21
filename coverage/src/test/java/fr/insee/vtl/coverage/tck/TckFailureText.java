package fr.insee.vtl.coverage.tck;

import fr.insee.vtl.coverage.model.Test;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Human-readable failure messages for CI logs (ASCII tables). No assertions — formatting only. */
public final class TckFailureText {

  static final int MAX_SCRIPT_CHARS = 4000;
  static final int MAX_TABLE_ROWS = 80;

  private TckFailureText() {}

  public static String structureMismatch(
      String displayPath,
      String outputName,
      Structured.DataStructure actual,
      Structured.DataStructure expected) {
    return String.format(
        "[%s] output `%s` — data structure differs%n%nTrevas (actual):%n%s%n%nTCK (expected):%n%s",
        displayPath,
        outputName,
        formatDataStructureTable(actual),
        formatDataStructureTable(expected));
  }

  /** Script failed during {@code engine.eval} (exception, not an output mismatch). */
  public static String executionError(String displayPath, Test test, Throwable error) {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("[%s] script execution failed%n", displayPath));
    appendExceptionSummary(sb, error);
    sb.append(System.lineSeparator());
    appendScript(sb, test.getScript());
    sb.append(System.lineSeparator());
    appendInputs(sb, test.getInput());
    return sb.toString();
  }

  public static String rowDataMismatch(
      String displayPath, String outputName, Test test, Dataset actualDs, Dataset expectedDs) {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("[%s] output `%s` — row data differs%n", displayPath, outputName));
    appendScript(sb, test.getScript());
    sb.append(System.lineSeparator());
    appendInputs(sb, test.getInput());
    sb.append(System.lineSeparator());
    sb.append("Trevas (actual):").append(System.lineSeparator());
    sb.append(formatDatasetTable(actualDs));
    sb.append(System.lineSeparator());
    sb.append("TCK (expected):").append(System.lineSeparator());
    sb.append(formatDatasetTable(expectedDs));
    return sb.toString();
  }

  private static void appendScript(StringBuilder sb, String script) {
    TckScriptText.appendFull(sb, script, MAX_SCRIPT_CHARS);
  }

  private static final int MAX_CAUSE_DEPTH = 4;
  private static final int MAX_TREVAS_FRAMES = 15;
  private static final int MAX_FALLBACK_FRAMES = 8;

  private static void appendExceptionSummary(StringBuilder sb, Throwable error) {
    if (error == null) {
      sb.append("(no exception)").append(System.lineSeparator());
      return;
    }
    Throwable root = rootCause(error);
    appendThrowableLine(sb, error, false);
    if (root != error) {
      sb.append("Root cause: ");
      appendThrowableLine(sb, root, true);
    }
    appendTrevasStackSection(sb, root);
  }

  private static Throwable rootCause(Throwable error) {
    Throwable cur = error;
    while (cur.getCause() != null && cur.getCause() != cur) {
      cur = cur.getCause();
    }
    return cur;
  }

  private static void appendThrowableLine(StringBuilder sb, Throwable t, boolean inline) {
    if (!inline) {
      sb.append("Exception: ");
    }
    sb.append(t.getClass().getName());
    String message = t.getMessage();
    if (message != null && !message.isBlank()) {
      sb.append(": ").append(message.trim());
    }
    sb.append(System.lineSeparator());
  }

  /**
   * All {@code fr.insee.vtl.*} frames with file:line for the report (not stripped by CI script).
   */
  private static void appendTrevasStackSection(StringBuilder sb, Throwable error) {
    StackTraceElement[] trace = error.getStackTrace();
    if (trace == null || trace.length == 0) {
      sb.append("Trevas stack: (empty stack trace)").append(System.lineSeparator());
      return;
    }
    List<StackTraceElement> trevas = new ArrayList<>();
    for (StackTraceElement frame : trace) {
      if (frame.getClassName().startsWith("fr.insee.vtl")) {
        trevas.add(frame);
      }
    }
    if (!trevas.isEmpty()) {
      sb.append("Trevas stack (file:line):").append(System.lineSeparator());
      int n = Math.min(trevas.size(), MAX_TREVAS_FRAMES);
      for (int i = 0; i < n; i++) {
        appendNumberedFrame(sb, i + 1, trevas.get(i));
      }
      if (trevas.size() > MAX_TREVAS_FRAMES) {
        sb.append("  … ")
            .append(trevas.size() - MAX_TREVAS_FRAMES)
            .append(" more Trevas frames omitted")
            .append(System.lineSeparator());
      }
      return;
    }
    sb.append("Trevas stack: (no fr.insee.vtl frame — showing call path):")
        .append(System.lineSeparator());
    int shown = 0;
    for (StackTraceElement frame : trace) {
      if (isNoiseFrame(frame.getClassName())) {
        continue;
      }
      if (shown >= MAX_FALLBACK_FRAMES) {
        break;
      }
      appendNumberedFrame(sb, shown + 1, frame);
      shown++;
    }
    if (shown == 0) {
      for (int i = 0; i < Math.min(5, trace.length); i++) {
        appendNumberedFrame(sb, i + 1, trace[i]);
      }
    }
  }

  private static void appendNumberedFrame(StringBuilder sb, int index, StackTraceElement frame) {
    sb.append("  ")
        .append(index)
        .append(". ")
        .append(frame.getClassName())
        .append(".")
        .append(frame.getMethodName())
        .append(" — ")
        .append(formatSourceLocation(frame))
        .append(System.lineSeparator());
  }

  private static String formatSourceLocation(StackTraceElement frame) {
    String file = frame.getFileName();
    if (file == null || file.isBlank()) {
      file = "(unknown file)";
    }
    int line = frame.getLineNumber();
    if (line > 0) {
      return file + ":" + line;
    }
    return file + " (line unknown)";
  }

  private static boolean isNoiseFrame(String className) {
    return className.startsWith("java.")
        || className.startsWith("jdk.")
        || className.startsWith("sun.")
        || className.startsWith("org.junit.")
        || className.startsWith("org.assertj.")
        || className.startsWith("org.apache.maven.")
        || className.startsWith("org.opentest4j.");
  }

  private static void appendInputs(StringBuilder sb, Map<String, Dataset> inputs) {
    sb.append("--- inputs ---").append(System.lineSeparator());
    if (inputs == null || inputs.isEmpty()) {
      sb.append("(none)").append(System.lineSeparator());
      return;
    }
    inputs.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .forEach(
            e -> {
              sb.append("« ").append(e.getKey()).append(" »").append(System.lineSeparator());
              sb.append(formatDatasetTable(e.getValue()));
              sb.append(System.lineSeparator());
            });
  }

  static String formatDataStructureTable(Structured.DataStructure ds) {
    if (ds == null || ds.isEmpty()) {
      return "(empty structure)";
    }
    List<String> headers = List.of("Column", "Type", "Role", "Nullable");
    List<List<String>> rows = new ArrayList<>();
    for (String key : ds.keySet()) {
      Structured.Component comp = ds.get(key);
      rows.add(
          List.of(
              comp.getName(),
              simpleTypeName(comp.getType()),
              String.valueOf(comp.getRole()),
              String.valueOf(comp.getNullable())));
    }
    return renderTable(headers, rows);
  }

  static String formatDatasetTable(Dataset dataset) {
    if (dataset == null) {
      return "(null dataset)";
    }
    Structured.DataStructure structure = dataset.getDataStructure();
    if (structure.isEmpty()) {
      return "(no columns)";
    }
    List<List<Object>> data = dataset.getDataAsList();
    List<String> headers = new ArrayList<>(structure.keySet());
    List<List<String>> rows = new ArrayList<>();
    int limit = Math.min(data.size(), MAX_TABLE_ROWS);
    for (int i = 0; i < limit; i++) {
      List<Object> row = data.get(i);
      List<String> cells = new ArrayList<>();
      for (Object v : row) {
        cells.add(formatCell(v));
      }
      rows.add(cells);
    }
    String table = renderTable(headers, rows);
    if (data.size() > MAX_TABLE_ROWS) {
      return table
          + System.lineSeparator()
          + "… ("
          + (data.size() - MAX_TABLE_ROWS)
          + " more rows not shown)";
    }
    return table;
  }

  private static String formatCell(Object v) {
    if (v == null) {
      return "";
    }
    return String.valueOf(v);
  }

  private static String simpleTypeName(Class<?> type) {
    if (type == null) {
      return "";
    }
    String n = type.getName();
    if (n.startsWith("java.lang.")) {
      return n.substring("java.lang.".length());
    }
    return n;
  }

  private static String renderTable(List<String> headers, List<List<String>> rows) {
    int cols = headers.size();
    int[] width = new int[cols];
    for (int c = 0; c < cols; c++) {
      width[c] = headers.get(c).length();
    }
    for (List<String> row : rows) {
      for (int c = 0; c < cols && c < row.size(); c++) {
        width[c] = Math.max(width[c], row.get(c).length());
      }
    }
    StringBuilder sb = new StringBuilder();
    for (int c = 0; c < cols; c++) {
      if (c > 0) {
        sb.append(" | ");
      }
      pad(sb, headers.get(c), width[c]);
    }
    sb.append(System.lineSeparator());
    for (int c = 0; c < cols; c++) {
      if (c > 0) {
        sb.append("-+-");
      }
      sb.append("-".repeat(width[c]));
    }
    sb.append(System.lineSeparator());
    for (List<String> row : rows) {
      for (int c = 0; c < cols; c++) {
        if (c > 0) {
          sb.append(" | ");
        }
        String cell = c < row.size() ? row.get(c) : "";
        pad(sb, cell, width[c]);
      }
      sb.append(System.lineSeparator());
    }
    return sb.toString();
  }

  private static void pad(StringBuilder sb, String s, int w) {
    sb.append(s);
    int pad = w - s.length();
    if (pad > 0) {
      sb.append(" ".repeat(pad));
    }
  }
}
