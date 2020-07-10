package fr.insee.vtl.engine.visitors.expression;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ComparisonExprTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testComparisonExpr() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("bool := true = true;");
        assertThat(context.getAttribute("bool")).isEqualTo(true);
        engine.eval("long := 6 = (3*2);");
        assertThat(context.getAttribute("long")).isEqualTo(true);
    }

    @Test
    void testInNotIn() throws ScriptException {

        engine.eval("res := \"string\" in {\"a\",\"list\",\"with\",\"string\"};");
        assertThat(engine.getContext().getAttribute("res")).isEqualTo(true);

        engine.eval("res := \"string\" in {\"a\",\"list\",\"with\",\"out string\"};");
        assertThat(engine.getContext().getAttribute("res")).isEqualTo(false);

        engine.getContext().setAttribute("var", 123L, ScriptContext.ENGINE_SCOPE);
        engine.eval("res := var in {1, 2, 3, 123};");

        assertThat(engine.getContext().getAttribute("res")).isEqualTo(true);

        assertThatThrownBy(() -> {
            engine.eval("res := var in {1, 2, 3, \"string is not number\"};");
        });

        assertThatThrownBy(() -> {
            engine.eval("res := \"string is not number\" in {1, 2, 3};");
        });


    }
}
