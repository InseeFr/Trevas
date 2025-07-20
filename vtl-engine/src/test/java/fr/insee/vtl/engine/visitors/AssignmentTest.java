package fr.insee.vtl.engine.visitors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import fr.insee.vtl.engine.samples.DatasetSamples;
import fr.insee.vtl.model.DataPointRuleset;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.HierarchicalRuleset;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import javax.script.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AssignmentTest {

  private ScriptEngine engine;

  @BeforeEach
  public void setUp() {
    engine = new ScriptEngineManager().getEngineByName("vtl");
  }

  @Test
  public void testAssignment() throws ScriptException {
    Bindings bindings = engine.createBindings();
    engine.eval("a := 1234;", bindings);
    engine.eval("b := 1234.1234;", bindings);
    engine.eval("c := true;", bindings);
    engine.eval("d := false;", bindings);
    engine.eval("e := \"foo\";", bindings);
    engine.eval("f := null;", bindings);

    //        assertThat(bindings).containsAllEntriesOf(Map.of(
    //                "a", 1234L,
    //                "b", 1234.1234,
    //                "c", true,
    //                "d", false,
    //                "e", "foo"
    //        ));
    //
    //        assertThat(bindings.get("f")).isNull();
  }

  @Test
  public void testDataPointRuleset() throws ScriptException {
    engine.eval(
        "define datapoint ruleset dpr1 (variable Id_3, Me_1) is "
            + "ruleA : when Id_3 = \"CREDIT\" then Me_1 >= 0 errorcode \"Bad credit\" errorlevel 2; "
            + "when Id_3 = \"DEBIT\" then Me_1 >= 0 errorcode \"Bad debit\" "
            + "end datapoint ruleset;");
    DataPointRuleset dpr1 = (DataPointRuleset) engine.getContext().getAttribute("dpr1");
    assertThat(dpr1.getRules()).hasSize(2);
    assertThat(dpr1.getVariables()).hasSize(2);
    assertThat(dpr1.getRules().get(0).getName()).isEqualTo("ruleA");
    assertThat(dpr1.getRules().get(1).getName()).isEqualTo("dpr1_2");
    assertThat(dpr1.getRules().get(0).getErrorLevelExpression().getType()).isEqualTo(Long.class);
  }

  @Test
  public void testDataPointRulesetWithValuedomain() throws ScriptException {
    engine.eval(
        "define datapoint ruleset dpr1 (valuedomain vd as VD) is "
            + "ruleA : VD = \"CREDIT\" errorcode \"Has to be CREDIT or DEBIT\" errorlevel 1 "
            + "end datapoint ruleset; ");
    DataPointRuleset dpr1 = (DataPointRuleset) engine.getContext().getAttribute("dpr1");
    assertThat(dpr1.getRules()).hasSize(1);
    assertThat(dpr1.getValuedomains()).hasSize(1);
    assertThat(dpr1.getRules().get(0).getName()).isEqualTo("ruleA");
    assertThat(dpr1.getRules().get(0).getErrorLevelExpression().getType()).isEqualTo(Long.class);
  }

  @Test
  public void testDataPointRulesetWithAlias() throws ScriptException {
    engine.eval(
        "define datapoint ruleset dpr1 (variable Id_3 as Id, Me_1) is "
            + "when Id = \"CREDIT\" then Me_1 >= 0 errorcode \"Bad credit\" errorlevel 2; "
            + "when Id = \"DEBIT\" then Me_1 >= 0 errorcode \"Bad debit\" "
            + "end datapoint ruleset; ");
    DataPointRuleset dpr1 = (DataPointRuleset) engine.getContext().getAttribute("dpr1");
    assertThat(dpr1.getAlias()).hasSize(1);
    assertThat(dpr1.getVariables()).contains("Id_3");
  }

  @Test
  public void testDataPointRulesetErrorTypes() throws ScriptException {
    engine.eval(
        "define datapoint ruleset dpr1 (variable Id_3 as Id, Me_1) is "
            + "when Id = \"CREDIT\" then Me_1 >= 0 errorcode true errorlevel \"level\"; "
            + "when Id = \"DEBIT\" then Me_1 >= 0 "
            + "end datapoint ruleset; ");
    DataPointRuleset dpr1 = (DataPointRuleset) engine.getContext().getAttribute("dpr1");
    assertThat(dpr1.getErrorCodeType()).isEqualTo(Boolean.class);
    assertThat(dpr1.getErrorLevelType()).isEqualTo(String.class);
    engine.eval(
        "define datapoint ruleset dpr2 (variable Id_3 as Id, Me_1) is "
            + "when Id = \"CREDIT\" then Me_1 >= 0 ; "
            + "when Id = \"DEBIT\" then Me_1 >= 0 "
            + "end datapoint ruleset; ");
    DataPointRuleset dpr2 = (DataPointRuleset) engine.getContext().getAttribute("dpr2");
    assertThat(dpr2.getErrorCodeType()).isEqualTo(String.class);
    assertThat(dpr2.getErrorLevelType()).isEqualTo(Long.class);
  }

  @Test
  public void testDataPointRulesetExceptions() {
    assertThatThrownBy(
            () ->
                engine.eval(
                    "define datapoint ruleset dpr1 (variable Id_3 as Id_1, Me_1) is "
                        + "when Id_1 = \"CREDIT\" then Me_1 >= 0 errorcode \"Bad credit\"; "
                        + "when Id_1 = \"DEBIT\" then Me_1 >= 0 errorcode true "
                        + "end datapoint ruleset; "))
        .hasMessageContaining("Error codes of rules have different types");
    assertThatThrownBy(
            () ->
                engine.eval(
                    "define datapoint ruleset dpr1 (variable Id_3 as Id_1, Me_1) is "
                        + "when Id_1 = \"CREDIT\" then Me_1 >= 0 errorcode \"Bad credit\" errorlevel 1; "
                        + "when Id_1 = \"DEBIT\" then Me_1 >= 0 errorcode \"Bad debit\" errorlevel \"2\""
                        + "end datapoint ruleset; "))
        .hasMessageContaining("Error levels of rules have different types");
  }

  @Test
  public void testMembership() throws ScriptException {
    engine.getContext().setAttribute("ds", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    engine.eval("res := ds#long1;");
    Dataset res = (Dataset) engine.getContext().getAttribute("res");
    assertThat(res.getDataStructure()).hasSize(2);

    assertThatThrownBy(() -> engine.eval("res := ds#baaaddd;"))
        .isInstanceOf(VtlScriptException.class)
        .hasMessage("column baaaddd not found in ds");
  }

  @Test
  public void checkHierarchy() throws ScriptException {

    String hierarchicalRulesetDef =
        """
                define hierarchical ruleset HR_1 (variable rule Me_1) is\s
                R010 : A = J + K + L errorlevel 5;
                R020 : B = M + N + O errorlevel 5;
                R030 : C = P + Q errorcode "XX" errorlevel 5;
                R040 : D = R + S errorlevel 1;
                R050 : E = T + U + V errorlevel 0;
                R060 : F = Y + W + Z errorlevel 7;
                R070 : G = B + C;
                R080 : H = D + E errorlevel 0;
                R090 : I = D + G errorcode "YY" errorlevel 0;
                R100 : M >= N errorlevel 5;
                R110 : M <= G errorlevel 5
                end hierarchical ruleset;\s
                """;

    engine.eval(hierarchicalRulesetDef);
    HierarchicalRuleset hr1 = (HierarchicalRuleset) engine.getContext().getAttribute("HR_1");
    assertThat(hr1.getRules()).hasSize(11);
  }
}
