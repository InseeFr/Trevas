package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.exceptions.InvalidTypeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ComparisonExprTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testNull() throws  ScriptException {

        ScriptContext context = engine.getContext();

        List<String> operators = List.of("=", "<>", "<", ">", "<=", ">=");
        List<String> values = List.of(
                "\"string\"", "1.1", "1", "true", "null"
        );

        for (String operator : operators) {
            // Left is null
            for (String value : values) {
                engine.eval("bool := null " + operator + " " + value + " ;");
                assertThat((Boolean) context.getAttribute("bool")).isNull();
            }

            // Right is null
            for (String value : values) {
                engine.eval("bool := " + value + " " + operator + ";");
                assertThat((Boolean) context.getAttribute("bool")).isNull();
            }
        }
    }

    @Test
    public void testComparisonExpr() throws ScriptException {
        ScriptContext context = engine.getContext();
        // EQ
        engine.eval("bool := true = true;");
        assertThat((Boolean) context.getAttribute("bool")).isTrue();
        engine.eval("long := 6 = (3*2);");
        assertThat((Boolean) context.getAttribute("long")).isTrue();
        // NEQ
        engine.eval("bool := true <> true;");
        assertThat((Boolean) context.getAttribute("bool")).isFalse();
        engine.eval("long := 6 <> (3*20);");
        assertThat((Boolean) context.getAttribute("long")).isTrue();
        // LT
        engine.eval("lt := 2 < 3;");
        assertThat((Boolean) context.getAttribute("lt")).isTrue();
        engine.eval("lt1 := 2.1 < 1.1;");
        assertThat((Boolean) context.getAttribute("lt1")).isFalse();
        // MT
        engine.eval("lt := 2 > 3;");
        assertThat((Boolean) context.getAttribute("lt")).isFalse();
        engine.eval("lt1 := 2.1 > 1.1;");
        assertThat((Boolean) context.getAttribute("lt1")).isTrue();
        // LE
        engine.eval("lt := 3 <= 3;");
        assertThat((Boolean) context.getAttribute("lt")).isTrue();
        engine.eval("lt1 := 2.1 <= 1.1;");
        assertThat((Boolean) context.getAttribute("lt1")).isFalse();
        // MT
        engine.eval("lt := 2 >= 3;");
        assertThat((Boolean) context.getAttribute("lt")).isFalse();
        engine.eval("lt1 := 2.1 >= 1.1;");
        assertThat((Boolean) context.getAttribute("lt1")).isTrue();
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
        assertThat((Boolean) engine.getContext().getAttribute("res")).isTrue();

        engine.eval("res := \"string\" in {\"a\",\"list\",\"with\",\"out string\"};");
        assertThat((Boolean) engine.getContext().getAttribute("res")).isFalse();

        engine.getContext().setAttribute("var", 123L, ScriptContext.ENGINE_SCOPE);
        engine.eval("res := var in {1, 2, 3, 123};");

        assertThat((Boolean) engine.getContext().getAttribute("res")).isTrue();

        assertThatThrownBy(() -> {
            engine.eval("res := var in {1, 2, 3, \"string is not number\"};");
        });

        assertThatThrownBy(() -> {
            engine.eval("res := \"string is not number\" in {1, 2, 3};");
        });


    }
}
