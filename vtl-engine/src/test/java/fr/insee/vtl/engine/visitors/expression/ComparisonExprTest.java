package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.exceptions.InvalidTypeException;
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
        // EQ
        engine.eval("bool := true = true;");
        assertThat(context.getAttribute("bool")).isEqualTo(true);
        engine.eval("long := 6 = (3*2);");
        assertThat(context.getAttribute("long")).isEqualTo(true);
        // NEQ
        engine.eval("bool := true <> true;");
        assertThat(context.getAttribute("bool")).isEqualTo(false);
        engine.eval("long := 6 <> (3*20);");
        assertThat(context.getAttribute("long")).isEqualTo(true);
        // LT
        engine.eval("lt := 2 < 3;");
        assertThat(context.getAttribute("lt")).isEqualTo(true);
        engine.eval("lt1 := 2.1 < 1.1;");
        assertThat(context.getAttribute("lt1")).isEqualTo(false);
        // MT
        engine.eval("lt := 2 > 3;");
        assertThat(context.getAttribute("lt")).isEqualTo(false);
        engine.eval("lt1 := 2.1 > 1.1;");
        assertThat(context.getAttribute("lt1")).isEqualTo(true);
        // LE
        engine.eval("lt := 3 <= 3;");
        assertThat(context.getAttribute("lt")).isEqualTo(true);
        engine.eval("lt1 := 2.1 <= 1.1;");
        assertThat(context.getAttribute("lt1")).isEqualTo(false);
        // MT
        engine.eval("lt := 2 >= 3;");
        assertThat(context.getAttribute("lt")).isEqualTo(false);
        engine.eval("lt1 := 2.1 >= 1.1;");
        assertThat(context.getAttribute("lt1")).isEqualTo(true);
    }

    @Test
    public void testComparisonExceptions() {
        assertThatThrownBy(() -> {
            engine.eval("s := \"ok\" <> true;");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type Boolean, expected true to be String");
        assertThatThrownBy(() -> {
            engine.eval("s := 2.1 < 3;");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type Long, expected 3 to be Double");
        assertThatThrownBy(() -> {
            engine.eval("s := 2 > 3.2;");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type Double, expected 3.2 to be Long");
        assertThatThrownBy(() -> {
            engine.eval("s := 2.1 <= 3;");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type Long, expected 3 to be Double");
        assertThatThrownBy(() -> {
            engine.eval("s := 2 >= 3.55;");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type Double, expected 3.55 to be Long");
    }

    @Test
    public void testInNotIn() throws ScriptException {

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
