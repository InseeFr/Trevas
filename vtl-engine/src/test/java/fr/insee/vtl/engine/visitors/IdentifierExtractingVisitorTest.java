package fr.insee.vtl.engine.visitors;

import static fr.insee.vtl.engine.utils.dag.DagDefineStatementsTest.parseScript;
import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.engine.utils.dag.DAGStatement;
import java.util.Set;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

/*
As workaround for https://github.com/InseeFr/Trevas/issues/457, as long the open points in here
are not clarified and https://github.com/InseeFr/Trevas/issues/355 is not implemented
the visitor is currently skipping varIds which would be components in the unadjusted VTL grammar
*/
class IdentifierExtractingVisitorTest {

  private final DAGBuildingVisitor.IdentifierExtractingVisitor visitor =
      new DAGBuildingVisitor.IdentifierExtractingVisitor();

  private static @NotNull Set<String> getAllNames(Set<DAGStatement.Identifier> result) {
    return result.stream().map(DAGStatement.Identifier::name).collect(Collectors.toSet());
  }

  @Test
  void testBasicAssignment() {
    String script = "ds1 := ds2;";
    Set<DAGStatement.Identifier> result = visitor.visit(parseScript(script));

    assertThat(result)
        .containsExactlyInAnyOrder(
            new DAGStatement.Identifier(DAGStatement.Identifier.Type.VARIABLE, "ds1"),
            new DAGStatement.Identifier(DAGStatement.Identifier.Type.VARIABLE, "ds2"));
  }

  @Test
  void testFilterClause() {
    String script = "ds1 := ds2 [filter component_a > 10];";
    Set<DAGStatement.Identifier> result = visitor.visit(parseScript(script));

    assertThat(result)
        .containsExactlyInAnyOrder(
            new DAGStatement.Identifier(DAGStatement.Identifier.Type.VARIABLE, "ds1"),
            new DAGStatement.Identifier(DAGStatement.Identifier.Type.VARIABLE, "ds2"));

    assertThat(getAllNames(result)).doesNotContain("component_a");
  }

  @Test
  void testCalcClause() {
    String script = "ds1 := ds2 [calc comp_new := comp_old + 5];";
    Set<DAGStatement.Identifier> result = visitor.visit(parseScript(script));

    assertThat(result)
        .containsExactlyInAnyOrder(
            new DAGStatement.Identifier(DAGStatement.Identifier.Type.VARIABLE, "ds1"),
            new DAGStatement.Identifier(DAGStatement.Identifier.Type.VARIABLE, "ds2"));

    assertThat(getAllNames(result)).doesNotContain("comp_new", "comp_old");
  }

  @Test
  void testMembershipExpression() {
    String script = "ds1 := ds2#component_a;";
    Set<DAGStatement.Identifier> result = visitor.visit(parseScript(script));

    assertThat(result)
        .containsExactlyInAnyOrder(
            new DAGStatement.Identifier(DAGStatement.Identifier.Type.VARIABLE, "ds1"),
            new DAGStatement.Identifier(DAGStatement.Identifier.Type.VARIABLE, "ds2"));

    assertThat(getAllNames(result)).doesNotContain("component_a");
  }

  @Test
  void testMin() {
    String script = "ds1 := min(ds2);";
    Set<DAGStatement.Identifier> result = visitor.visit(parseScript(script));

    assertThat(result)
        .containsExactlyInAnyOrder(
            new DAGStatement.Identifier(DAGStatement.Identifier.Type.VARIABLE, "ds1"),
            new DAGStatement.Identifier(DAGStatement.Identifier.Type.VARIABLE, "ds2"));
  }

  @Test
  void testAnalyticFunctions() {
    String script = "ds1 := ds2[calc x := lag(other, 1) over (partition by comp_a)];";
    Set<DAGStatement.Identifier> result = visitor.visit(parseScript(script));

    assertThat(result)
        .containsExactlyInAnyOrder(
            new DAGStatement.Identifier(DAGStatement.Identifier.Type.VARIABLE, "ds1"),
            new DAGStatement.Identifier(DAGStatement.Identifier.Type.VARIABLE, "ds2"));

    assertThat(getAllNames(result)).doesNotContain("comp_a", "x", "other");
  }

  @Test
  void testAggrClause() {
    String script = "ds1 := ds2 [aggregate comp_sum := sum(comp_val) group by comp_grp];";
    Set<DAGStatement.Identifier> result = visitor.visit(parseScript(script));

    assertThat(result)
        .containsExactlyInAnyOrder(
            new DAGStatement.Identifier(DAGStatement.Identifier.Type.VARIABLE, "ds1"),
            new DAGStatement.Identifier(DAGStatement.Identifier.Type.VARIABLE, "ds2"));

    assertThat(getAllNames(result)).doesNotContain("comp_sum", "comp_val", "comp_grp");
  }

  @Test
  void testJoinWithAliases() {
    String script = "ds1 := inner_join(ds2 as a, ds3 as b using comp_id);";
    Set<DAGStatement.Identifier> result = visitor.visit(parseScript(script));

    assertThat(result)
        .containsExactlyInAnyOrder(
            new DAGStatement.Identifier(DAGStatement.Identifier.Type.VARIABLE, "ds1"),
            new DAGStatement.Identifier(DAGStatement.Identifier.Type.VARIABLE, "ds2"),
            new DAGStatement.Identifier(DAGStatement.Identifier.Type.VARIABLE, "ds3"));
    assertThat(getAllNames(result)).doesNotContain("a", "b", "comp_id");
  }

  @Test
  void testValidationRulesets() {
    String script = "ds1 := check_datapoint(ds2, my_ruleset);";
    Set<DAGStatement.Identifier> result = visitor.visit(parseScript(script));

    assertThat(result)
        .containsExactlyInAnyOrder(
            new DAGStatement.Identifier(DAGStatement.Identifier.Type.VARIABLE, "ds1"),
            new DAGStatement.Identifier(DAGStatement.Identifier.Type.VARIABLE, "ds2"),
            new DAGStatement.Identifier(
                DAGStatement.Identifier.Type.RULESET_DATAPOINT, "my_ruleset"));
  }

  @Test
  void testHierarchicalValidation() {
    String script = "ds1 := check_hierarchy(ds2, my_h_ruleset);";
    Set<DAGStatement.Identifier> result = visitor.visit(parseScript(script));

    assertThat(result)
        .containsExactlyInAnyOrder(
            new DAGStatement.Identifier(DAGStatement.Identifier.Type.VARIABLE, "ds1"),
            new DAGStatement.Identifier(DAGStatement.Identifier.Type.VARIABLE, "ds2"),
            new DAGStatement.Identifier(
                DAGStatement.Identifier.Type.RULESET_HIERARCHICAL, "my_h_ruleset"));
  }

  @Test
  void testHavingClause() {
    String script = "ds1 := ds2 [aggregate sum_val := sum(val) group by grp having sum_val > 0];";
    Set<DAGStatement.Identifier> result = visitor.visit(parseScript(script));

    assertThat(getAllNames(result)).containsExactlyInAnyOrder("ds1", "ds2");
    assertThat(getAllNames(result)).doesNotContain("sum_val", "val", "grp");
  }

  @Test
  void testJoinApply() {
    String script = "ds1 := inner_join(ds2 as a, ds3 as b apply comp_new := comp_old * 2);";
    Set<DAGStatement.Identifier> result = visitor.visit(parseScript(script));

    assertThat(getAllNames(result)).containsExactlyInAnyOrder("ds1", "ds2", "ds3");
    assertThat(getAllNames(result)).doesNotContain("comp_new", "comp_old", "a", "b");
  }

  @Test
  void testRankAnalytic() {
    String script = "ds1 := ds2 [calc r := rank() over (partition by p order by o)];";
    Set<DAGStatement.Identifier> result = visitor.visit(parseScript(script));

    assertThat(getAllNames(result)).containsExactlyInAnyOrder("ds1", "ds2");
    assertThat(getAllNames(result)).doesNotContain("r", "p", "o");
  }

  @Test
  void testRatioToReportAnalytic() {
    String script = "ds1 := ratio_to_report(ds2, comp_a) over (partition by comp_b);";
    Set<DAGStatement.Identifier> result = visitor.visit(parseScript(script));

    assertThat(getAllNames(result)).containsExactlyInAnyOrder("ds1", "ds2");
    assertThat(getAllNames(result)).doesNotContain("comp_a", "comp_b");
  }

  @Test
  void testIgnoreInnerScopedVars() {
    var customVisitor = new DAGBuildingVisitor.IdentifierExtractingVisitor(Set.of("ds_ignored"));
    String script = "ds1 := ds2 + ds_ignored;";
    Set<DAGStatement.Identifier> result = customVisitor.visit(parseScript(script));

    assertThat(getAllNames(result)).containsExactlyInAnyOrder("ds1", "ds2");
    assertThat(getAllNames(result)).doesNotContain("ds_ignored");
  }

  @Test
  void testComplexChainedClauses() {
    String script =
        "ds1 := ds2 [filter c1 > 0] [calc c2 := c1 + 1] [calc c3 := sum(c2) over (partition by c1)];";
    Set<DAGStatement.Identifier> result = visitor.visit(parseScript(script));

    assertThat(getAllNames(result)).containsExactlyInAnyOrder("ds1", "ds2");
    assertThat(getAllNames(result)).doesNotContain("c1", "c2", "c3");
  }
}
