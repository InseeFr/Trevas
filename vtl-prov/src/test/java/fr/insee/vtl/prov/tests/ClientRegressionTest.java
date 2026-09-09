package fr.insee.vtl.prov.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import fr.insee.vtl.prov.extract.ProvenanceExtractor;
import fr.insee.vtl.prov.ir.ProvGraph;
import fr.insee.vtl.testutils.InputDataset;
import fr.insee.vtl.testutils.InputDirectives;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

/**
 * Release-gate: Trevas Desktop demo shapes + nested producers under arithmetic must extract without
 * {@code unsupported: …} and must not silently emit {@code kind=scalar} for dataset products.
 * ({@code output} is a reserved keyword — persist uses {@code output.name <- …}.)
 */
class ClientRegressionTest {

  private static final ProvenanceExtractor EXTRACTOR = new ProvenanceExtractor();

  private static final String DS =
      """
      // $input taxis: vendor_id STRING IDENTIFIER, total_amount NUMBER MEASURE, trip_distance NUMBER MEASURE
      """;

  @Test
  void desktopFourthTestBis_ratioToReportTimesScalar() {
    String script =
        DS
            + """
            taxis2 := taxis [calc identifier vendor_id := vendor_id] [keep total_amount];
            output.fourth_test_bis <- ratio_to_report(taxis2 over(partition by vendor_id)) * 1000000;
            """;
    ProvGraph graph = extract(script);
    assertDatasetProduct(graph, "output.fourth_test_bis");
  }

  @Test
  void desktopFourthTestV1_leftJoinCalcPercent() {
    String script =
        DS
            + """
            taxis2 := taxis [calc identifier vendor_id := vendor_id, amount := cast(total_amount, number)] [drop total_amount];
            total_amount_ds := taxis [calc total_amount := cast(total_amount, number)]
                          [aggr total_amount := sum(total_amount) group by vendor_id]
                          [calc total_amount := floor(total_amount)];
            output.fourth_test <- left_join(taxis2, total_amount_ds using vendor_id)
                        [keep vendor_id, amount, total_amount]
                        [calc percent := round(amount / total_amount * 100, 1)]
                        [drop amount, total_amount];
            """;
    ProvGraph graph = extract(script);
    assertDatasetProduct(graph, "output.fourth_test");
  }

  @Test
  void desktopSecondAndThird_filterAggr() {
    String script =
        DS
            + """
            second := taxis [calc trip_distance := cast(trip_distance, number)] [filter trip_distance > 10];
            third := taxis [calc total_amount := cast(total_amount, number)]
                           [aggr amount := sum(total_amount) group by vendor_id]
                           [calc amount := floor(amount)];
            """;
    ProvGraph graph = extract(script);
    assertDatasetProduct(graph, "second");
    assertDatasetProduct(graph, "third");
  }

  @Test
  void udoDatasetBodyWithoutReturns_isNotSilentScalar() {
    String script =
        """
        // $input ds1: id STRING IDENTIFIER, m1 INTEGER MEASURE
        // $input ds2: id STRING IDENTIFIER, m1 INTEGER MEASURE
        define operator boom() is
          union(ds1, ds2)
        end operator;
        out := boom();
        """;
    ProvGraph graph = extract(script);
    assertDatasetProduct(graph, "out");
  }

  @Test
  void udoUnionFormalsWithoutReturns_extracts() {
    String script =
        """
        // $input ds1: id STRING IDENTIFIER, m1 INTEGER MEASURE
        // $input ds2: id STRING IDENTIFIER, m1 INTEGER MEASURE
        define operator u (a dataset, b dataset) is
          union(a, b)
        end operator;
        out := u(ds1, ds2) * 1;
        """;
    ProvGraph graph = extract(script);
    assertDatasetProduct(graph, "out");
  }

  @TestFactory
  Stream<DynamicTest> producerTimesScalar() {
    String hdr =
        """
        // $input ds1: id STRING IDENTIFIER, m1 INTEGER MEASURE
        // $input ds2: id STRING IDENTIFIER, m1 INTEGER MEASURE
        """;
    Map<String, String> cases =
        Map.of(
            "analytic", "out := ratio_to_report(ds1 over(partition by id)) * 2;",
            "sum-aggr", "out := sum(ds1 group by id) * 2;",
            "count-aggr", "out := count(ds1 group by id) * 1;",
            "union", "out := union(ds1, ds2) * 1;",
            "abs-ds", "out := abs(ds1) * 1;",
            "clause-arith", "out := ds1[calc m1 := m1 * 2] * 1;",
            "unary-analytic", "out := - ratio_to_report(ds1 over(partition by id));",
            "paren-analytic", "out := (ratio_to_report(ds1 over(partition by id))) * 1;");
    return cases.entrySet().stream()
        .map(
            e ->
                DynamicTest.dynamicTest(
                    e.getKey(),
                    () -> {
                      ProvGraph graph = extract(hdr + e.getValue());
                      assertDatasetProduct(graph, "out");
                    }));
  }

  @Test
  void scalarUdoStillScalar() {
    String script =
        """
        define operator add1 (x integer) returns integer is
          x + 1
        end operator;
        y := add1(1);
        """;
    ProvGraph graph = extract(script);
    assertThat(latestKind(graph, "y")).isEqualTo("scalar");
  }

  private static ProvGraph extract(String script) {
    List<InputDataset> inputs = InputDirectives.parse(script);
    assertThatCode(() -> EXTRACTOR.extract(script, inputs))
        .as("extract must not throw unsupported")
        .doesNotThrowAnyException();
    return EXTRACTOR.extract(script, inputs);
  }

  private static void assertDatasetProduct(ProvGraph graph, String name) {
    assertThat(latestKind(graph, name))
        .as("assignment product %s must be dataset, not silent scalar", name)
        .isEqualTo("dataset");
  }

  private static final Pattern VERSIONED = Pattern.compile("^(.+)@(\\d+)$");

  /** Latest versioned dataset/scalar id {@code name@N} (ignores {@code name@N.comp}). */
  private static String latestKind(ProvGraph graph, String name) {
    String best = null;
    int bestN = -1;
    for (Map.Entry<String, Map<String, String>> e : graph.vertices().entrySet()) {
      Matcher m = VERSIONED.matcher(e.getKey());
      if (!m.matches() || !name.equals(m.group(1))) {
        continue;
      }
      String kind = e.getValue().get("kind");
      if (!"dataset".equals(kind) && !"scalar".equals(kind)) {
        continue;
      }
      int n = Integer.parseInt(m.group(2));
      if (n > bestN) {
        bestN = n;
        best = kind;
      }
    }
    assertThat(best).as("missing dataset/scalar vertex for %s", name).isNotNull();
    return best;
  }
}
