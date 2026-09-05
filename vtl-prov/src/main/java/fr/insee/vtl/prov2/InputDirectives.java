package fr.insee.vtl.prov2;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses {@code $input} directives from VTL fixture comments (spec 20260729_01). One-liner and
 * table forms; unknown directives elsewhere are ignored by callers that only invoke this for
 * {@code $input}.
 */
public final class InputDirectives {

  private static final Pattern INPUT_ONE_LINER =
      Pattern.compile("^\\s*//\\s*\\$input\\s+(\\S+)\\s*:\\s*(.+?)\\s*$");
  private static final Pattern BLOCK_COMMENT = Pattern.compile("/\\*(.*?)\\*/", Pattern.DOTALL);
  private static final Pattern INPUT_NAME = Pattern.compile("^\\$input\\s+(\\S+)\\s*$");
  private static final Pattern SEPARATOR_CELL = Pattern.compile("-+");

  private InputDirectives() {}

  public static List<InputDataset> parse(String script) {
    List<InputDataset> inputs = new ArrayList<>();
    for (String line : script.lines().toList()) {
      Matcher m = INPUT_ONE_LINER.matcher(line);
      if (!m.matches()) {
        continue;
      }
      inputs.add(new InputDataset(m.group(1), parseColumns(m.group(2)), List.of()));
    }
    Matcher blocks = BLOCK_COMMENT.matcher(script);
    while (blocks.find()) {
      InputDataset table = parseTableBlock(blocks.group(1));
      if (table != null) {
        inputs.add(table);
      }
    }
    return inputs;
  }

  private static InputDataset parseTableBlock(String raw) {
    List<String> lines = new ArrayList<>();
    for (String line : raw.lines().toList()) {
      String stripped = line.replaceFirst("^\\s*\\*?\\s?", "").strip();
      if (!stripped.isEmpty()) {
        lines.add(stripped);
      }
    }
    if (lines.isEmpty()) {
      return null;
    }
    Matcher name = INPUT_NAME.matcher(lines.get(0));
    if (!name.matches()) {
      return null;
    }
    List<String> tableLines = lines.subList(1, lines.size()).stream().filter(l -> l.startsWith("|")).toList();
    if (tableLines.size() < 3) {
      throw new IllegalArgumentException(
          "malformed $input table for " + name.group(1) + ": need name/type/role header rows");
    }
    List<String> names = cells(tableLines.get(0));
    List<String> types = cells(tableLines.get(1));
    List<String> roles = cells(tableLines.get(2));
    if (names.size() != types.size() || names.size() != roles.size()) {
      throw new IllegalArgumentException(
          "malformed $input table for " + name.group(1) + ": header row widths differ");
    }
    List<InputDataset.Column> columns = new ArrayList<>();
    for (int i = 0; i < names.size(); i++) {
      columns.add(new InputDataset.Column(names.get(i), types.get(i), roles.get(i), Map.of()));
    }
    int dataStart = 3;
    if (tableLines.size() > 3 && isSeparator(tableLines.get(3))) {
      dataStart = 4;
    }
    List<List<String>> rows = new ArrayList<>();
    for (int r = dataStart; r < tableLines.size(); r++) {
      List<String> row = cells(tableLines.get(r));
      if (row.size() != names.size()) {
        throw new IllegalArgumentException(
            "malformed $input table for "
                + name.group(1)
                + ": data row width "
                + row.size()
                + " != "
                + names.size());
      }
      rows.add(List.copyOf(row));
    }
    return new InputDataset(name.group(1), List.copyOf(columns), List.copyOf(rows));
  }

  private static List<InputDataset.Column> parseColumns(String structure) {
    List<InputDataset.Column> columns = new ArrayList<>();
    for (String part : structure.split(",")) {
      String[] tokens = part.strip().split("\\s+");
      if (tokens.length < 3) {
        throw new IllegalArgumentException("malformed $input column: '" + part.strip() + "'");
      }
      Map<String, String> attrs = new LinkedHashMap<>();
      for (int i = 3; i < tokens.length; i++) {
        String[] kv = tokens[i].split("=", 2);
        attrs.put(kv[0], kv.length > 1 ? kv[1] : "");
      }
      columns.add(new InputDataset.Column(tokens[0], tokens[1], tokens[2], attrs));
    }
    return columns;
  }

  private static List<String> cells(String line) {
    String[] parts = line.split("\\|", -1);
    List<String> out = new ArrayList<>();
    for (int i = 1; i < parts.length - 1; i++) {
      out.add(parts[i].strip());
    }
    return out;
  }

  private static boolean isSeparator(String line) {
    List<String> parts = cells(line);
    if (parts.isEmpty()) {
      return false;
    }
    for (String cell : parts) {
      if (!SEPARATOR_CELL.matcher(cell).matches()) {
        return false;
      }
    }
    return true;
  }
}
