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

public class ArithmeticExprTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testNull() throws ScriptException {

        ScriptContext context = engine.getContext();

        List<String> operators = List.of("+", "-", "/", "*");
        List<String> values = List.of(
                "1.1", "1", "null"
        );

        for (String operator : operators) {
            // Left is null
            for (String value : values) {
                engine.eval("res := null " + operator + " " + value + " ;");
                assertThat((Boolean) context.getAttribute("res")).isNull();
            }

            // Right is null
            for (String value : values) {
                engine.eval("res := " + value + " " + operator + " null ;");
                assertThat((Boolean) context.getAttribute("res")).isNull();
            }
        }
    }

    @Test
    public void testArithmeticExpr() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("mul := 2 * 3;");
        assertThat(context.getAttribute("mul")).isEqualTo(6L);

        engine.eval("mul := 1.5 * 2;");
        assertThat(context.getAttribute("mul")).isEqualTo(3.0);
        engine.eval("mul := 2 * 1.5;");
        assertThat(context.getAttribute("mul")).isEqualTo(3.0);
        engine.eval("mul := 2.0 * 1.5;");
        assertThat(context.getAttribute("mul")).isEqualTo(3.0);

        engine.eval("div := 6 / 3;");
        assertThat(context.getAttribute("div")).isEqualTo(2.0);
        engine.eval("div := 1 / 0.5;");
        assertThat(context.getAttribute("div")).isEqualTo(2.0);
        engine.eval("div := 2.0 / 1;");
        assertThat(context.getAttribute("div")).isEqualTo(2.0);
        engine.eval("div := 3.0 / 1.5;");
        assertThat(context.getAttribute("div")).isEqualTo(2.0);
    }

    @Test
    public void testArithmeticWithVariables() throws ScriptException {
        ScriptContext context = engine.getContext();
        context.setAttribute("two", Integer.valueOf(2), ScriptContext.ENGINE_SCOPE);
        context.setAttribute("three", Integer.valueOf(3), ScriptContext.ENGINE_SCOPE);

        engine.eval("mul := two * three;");
        assertThat(context.getAttribute("mul")).isEqualTo(6L);

        context.setAttribute("onePFive", Float.valueOf(1.5F), ScriptContext.ENGINE_SCOPE);

        engine.eval("mul := onePFive * two;");
        assertThat(context.getAttribute("mul")).isEqualTo(3.0);

        context.setAttribute("twoLong", Long.valueOf(2L), ScriptContext.ENGINE_SCOPE);
        context.setAttribute("onePFiveDouble", Double.valueOf(1.5D), ScriptContext.ENGINE_SCOPE);

        engine.eval("mul := twoLong * onePFiveDouble;");
        assertThat(context.getAttribute("mul")).isEqualTo(3.0);
    }

    @Test
    public void testUnaryExpr() throws ScriptException {
        ScriptContext context = engine.getContext();

        engine.eval("plus := +1;");
        assertThat(context.getAttribute("plus")).isEqualTo(1L);
        engine.eval("plus := + 1.5;");
        assertThat(context.getAttribute("plus")).isEqualTo(1.5D);

        engine.eval("plus := -1;");
        assertThat(context.getAttribute("plus")).isEqualTo(-1L);
        engine.eval("plus := - 1.5;");
        assertThat(context.getAttribute("plus")).isEqualTo(-1.5D);

        assertThatThrownBy(() -> {
            engine.eval("plus := + \"ko\";");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type String, expected Number");
        assertThatThrownBy(() -> {
            engine.eval("minus := - \"ko\";");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type String, expected Number");
    }
}
