package fr.insee.vtl.prov2.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.DynamicContainer.dynamicContainer;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import fr.insee.vtl.prov2.InputDataset;
import fr.insee.vtl.prov2.ProvenanceExtractor;
import fr.insee.vtl.prov2.StatementWalkExtractor;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.TestFactory;

/**
 * Golden-corpus harness (specs/20260729_02_work-breakdown.md, PR-1).
 *
 * <p>Each folder under {@code vtl-prov/tests/} containing an {@code expected.dot} becomes a test
 * container with two tests:
 *
 * <ul>
 *   <li><b>golden self-check</b> — lints the fixture itself (DOT parses, node conventions hold,
 *       {@code $input} directives are consistent with the golden). Runs green with no extractor.
 *   <li><b>provenance</b> — runs {@link StatementWalkExtractor} and compares its graph to the
 *       golden. Cases fail until extraction supports them: the failing count is the backlog.
 * </ul>
 */
public class ProvenanceTests {

  private static final ProvenanceExtractor EXTRACTOR = new StatementWalkExtractor();

  private static final Set<String> KINDS = Set.of("dataset", "variable", "expression");
  private static final Set<String> ROLES = Set.of("IDENTIFIER", "MEASURE", "ATTRIBUTE");

  private static final Pattern INPUT_ONE_LINER =
      Pattern.compile("^\\s*//\\s*\\$input\\s+(\\S+)\\s*:\\s*(.+?)\\s*$");
  private static final Pattern INPUT_ANY = Pattern.compile("\\$input\\s+(\\S+)");
  private static final Pattern STMT_INDEX = Pattern.compile("(?:@|^e|^#s)(\\d+)");

  @TestFactory
  Stream<DynamicContainer> corpus() throws IOException {
    try (Stream<Path> entries = Files.list(corpusDir())) {
      List<Path> cases =
          entries.filter(d -> Files.exists(d.resolve("expected.dot"))).sorted().toList();
      assertThat(cases).as("corpus folders in " + corpusDir()).isNotEmpty();
      return cases.stream()
          .map(
              dir ->
                  dynamicContainer(
                      dir.getFileName().toString(),
                      Stream.of(
                          dynamicTest("golden self-check", () -> selfCheck(dir)),
                          dynamicTest("provenance", () -> extractionCheck(dir)))))
          .toList()
          .stream();
    }
  }

  private static Path corpusDir() {
    // Surefire working directory is the module dir; be lenient for IDE runs from the repo root.
    for (Path candidate : List.of(Path.of("tests"), Path.of("vtl-prov", "tests"))) {
      if (Files.isDirectory(candidate)) {
        return candidate;
      }
    }
    throw new IllegalStateException("corpus directory vtl-prov/tests not found");
  }

  // --- provenance assertion -----------------------------------------------------------------

  private void extractionCheck(Path dir) throws IOException {
    String name = dir.getFileName().toString();
    String script = Files.readString(dir.resolve("input.vtl"));
    Graph expected = Graph.fromDot(dir.resolve("expected.dot"));
    Graph actual = Graph.from(EXTRACTOR.extract(script, parseInputs(script)));
    GraphAssert.assertThat(actual).as("provenance graph for " + name).isSameGraphAs(expected);
  }

  // --- golden self-check --------------------------------------------------------------------

  private void selfCheck(Path dir) throws IOException {
    List<String> problems = new ArrayList<>();
    Path vtl = dir.resolve("input.vtl");
    if (!Files.exists(vtl)) {
      problems.add("input.vtl is missing");
    }
    Graph golden = Graph.fromDot(dir.resolve("expected.dot"));
    golden.vertices().forEach((id, attrs) -> checkVertex(id, attrs, problems));

    if (Files.exists(vtl)) {
      String script = Files.readString(vtl);
      checkDirectives(script, golden, problems);
      checkStatements(script, golden, problems);
    }
    assertThat(problems).as("golden self-check for " + dir.getFileName()).isEmpty();
  }

  private void checkVertex(String id, SortedMap<String, String> attrs, List<String> problems) {
    String kind = attrs.get("kind");
    if (kind == null) {
      problems.add(id + ": no kind attribute (referenced by an edge but never declared?)");
      return;
    }
    if (!KINDS.contains(kind)) {
      problems.add(id + ": unknown kind '" + kind + "'");
    }
    if ("variable".equals(kind)) {
      String dataset = attrs.get("dataset");
      if (dataset == null) {
        problems.add(id + ": variable without dataset attribute");
      } else if (!id.startsWith(dataset + ".")) {
        problems.add(id + ": id does not match its dataset attribute '" + dataset + "'");
      }
      if (!ROLES.contains(attrs.getOrDefault("role", ""))) {
        problems.add(id + ": variable with missing/unknown role '" + attrs.get("role") + "'");
      }
    }
    if ("expression".equals(kind) && attrs.get("src") == null) {
      problems.add(id + ": expression without src attribute");
    }
  }

  /** $input directives vs the binding datasets ("...@0") the golden declares. */
  private void checkDirectives(String script, Graph golden, List<String> problems) {
    List<InputDataset> inputs = parseInputs(script);
    boolean hasTableForm =
        INPUT_ANY.matcher(stripLineComments(script)).find(); // $input inside /* */ block
    Set<String> declared = new java.util.HashSet<>();
    inputs.forEach(i -> declared.add(i.name()));

    golden
        .vertices()
        .forEach(
            (id, attrs) -> {
              if ("dataset".equals(attrs.get("kind")) && id.endsWith("@0")) {
                String name = id.substring(0, id.length() - 2);
                if (!declared.contains(name) && !hasTableForm) {
                  problems.add(id + ": binding dataset has no $input directive");
                }
              }
              if ("variable".equals(attrs.get("kind"))) {
                String dataset = attrs.getOrDefault("dataset", "");
                if (dataset.endsWith("@0")) {
                  String dsName = dataset.substring(0, dataset.length() - 2);
                  inputs.stream()
                      .filter(i -> i.name().equals(dsName))
                      .findFirst()
                      .flatMap(
                          i ->
                              i.columns().stream()
                                  .filter(c -> id.equals(dataset + "." + c.name()))
                                  .findFirst())
                      .ifPresent(
                          column -> {
                            if (!column.role().equals(attrs.get("role"))) {
                              problems.add(
                                  id + ": role differs from $input (" + column.role() + ")");
                            }
                            if (!column.type().equals(attrs.get("type"))) {
                              problems.add(
                                  id + ": type differs from $input (" + column.type() + ")");
                            }
                          });
                }
              }
            });
    inputs.forEach(
        i -> {
          if (!golden.vertices().containsKey(i.name() + "@0")) {
            problems.add("$input " + i.name() + " is never used in expected.dot");
          }
        });
  }

  /** Script/golden statement consistency; catches drift like a statement added to one side only. */
  private void checkStatements(String script, Graph golden, List<String> problems) {
    String code = stripComments(script).strip();
    if (!code.isEmpty() && !code.endsWith(";")) {
      problems.add("input.vtl: script does not end with ';' (unterminated statement?)");
    }
    long statementCount = code.chars().filter(c -> c == ';').count();
    int maxIndex =
        golden.vertices().keySet().stream()
            .mapToInt(ProvenanceTests::statementIndex)
            .max()
            .orElse(0);
    if (maxIndex > statementCount) {
      problems.add(
          "expected.dot references statement "
              + maxIndex
              + " but input.vtl has only "
              + statementCount
              + " statement(s)");
    }
  }

  private static int statementIndex(String id) {
    Matcher m = STMT_INDEX.matcher(id);
    int max = 0;
    while (m.find()) {
      max = Math.max(max, Integer.parseInt(m.group(1)));
    }
    return max;
  }

  // --- $input parsing (one-liner form only; table form arrives with the pivot PR) ------------

  static List<InputDataset> parseInputs(String script) {
    List<InputDataset> inputs = new ArrayList<>();
    for (String line : script.lines().toList()) {
      Matcher m = INPUT_ONE_LINER.matcher(line);
      if (!m.matches()) {
        continue;
      }
      List<InputDataset.Column> columns = new ArrayList<>();
      for (String part : m.group(2).split(",")) {
        String[] tokens = part.strip().split("\\s+");
        if (tokens.length < 3) {
          throw new IllegalArgumentException("malformed $input column: '" + part.strip() + "'");
        }
        Map<String, String> attrs = new java.util.LinkedHashMap<>();
        for (int i = 3; i < tokens.length; i++) {
          String[] kv = tokens[i].split("=", 2);
          attrs.put(kv[0], kv.length > 1 ? kv[1] : "");
        }
        columns.add(new InputDataset.Column(tokens[0], tokens[1], tokens[2], attrs));
      }
      inputs.add(new InputDataset(m.group(1), columns));
    }
    return inputs;
  }

  private static String stripComments(String script) {
    return stripLineComments(script.replaceAll("(?s)/\\*.*?\\*/", " "));
  }

  private static String stripLineComments(String script) {
    return script.replaceAll("(?m)//.*$", "");
  }
}
