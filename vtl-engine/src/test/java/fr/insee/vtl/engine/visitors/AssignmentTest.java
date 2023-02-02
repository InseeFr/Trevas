package fr.insee.vtl.engine.visitors;

import fr.insee.vtl.model.DataPointRuleset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import static org.assertj.core.api.Assertions.assertThat;

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
        engine.eval("define datapoint ruleset dpr1 (variable Id_3, Me_1) is " +
                "ruleA : when Id_3 = \"CREDIT\" then Me_1 >= 0 errorcode \"Bad credit\" errorlevel 2; " +
                "when Id_3 = \"DEBIT\" then Me_1 >= 0 errorcode \"Bad debit\" " +
                "end datapoint ruleset; ");
        DataPointRuleset dpr1 = (DataPointRuleset) engine.getContext().getAttribute("dpr1");
        assertThat(dpr1.getRules()).hasSize(2);
        assertThat(dpr1.getVariables()).hasSize(2);
        assertThat(dpr1.getRules().get(0).getName()).isEqualTo("ruleA");
        assertThat(dpr1.getRules().get(1).getName()).isEqualTo("dpr1_2");
    }

    @Test
    public void testDataPointRulesetWithAlias() throws ScriptException {
        engine.eval("define datapoint ruleset dpr1 (variable Id_3 as Id, Me_1) is " +
                "when Id = \"CREDIT\" then Me_1 >= 0 errorcode \"Bad credit\" errorlevel 2; " +
                "when Id = \"DEBIT\" then Me_1 >= 0 errorcode \"Bad debit\" " +
                "end datapoint ruleset; ");
        DataPointRuleset dpr1 = (DataPointRuleset) engine.getContext().getAttribute("dpr1");
        assertThat(dpr1.getAlias()).hasSize(1);
        assertThat(dpr1.getVariables()).contains("Id_3");
    }

}