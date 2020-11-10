package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.InvalidTypeException;
import org.assertj.core.data.Offset;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class NumericFunctionsTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testCeil() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("a := ceil(3.14159);");
        assertThat(context.getAttribute("a")).isEqualTo(4L);
        engine.eval("b := ceil(15);");
        assertThat(context.getAttribute("b")).isEqualTo(15L);
        engine.eval("c := ceil(-3.1415);");
        assertThat(context.getAttribute("c")).isEqualTo(-3L);
        engine.eval("d := ceil(-0.1415);");
        assertThat(context.getAttribute("d")).isEqualTo(0L);
        assertThatThrownBy(() -> {
            engine.eval("e := ceil(\"ko\");");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type String, expected \"ko\" to be Double or Long");
    }

    @Test
    public void testFloor() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("a := floor(3.14159);");
        assertThat(context.getAttribute("a")).isEqualTo(3L);
        engine.eval("b := floor(15);");
        assertThat(context.getAttribute("b")).isEqualTo(15L);
        engine.eval("c := floor(-3.1415);");
        assertThat(context.getAttribute("c")).isEqualTo(-4L);
        engine.eval("d := floor(-0.1415);");
        assertThat(context.getAttribute("d")).isEqualTo(-1L);
        assertThatThrownBy(() -> {
            engine.eval("e := floor(\"ko\");");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type String, expected \"ko\" to be Double or Long");
    }

    @Test
    public void testAbs() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("a := abs(5.5);");
        assertThat(context.getAttribute("a")).isEqualTo(5.5D);
        engine.eval("b := abs(-5.5);");
        assertThat(context.getAttribute("b")).isEqualTo(5.5D);
        assertThatThrownBy(() -> {
            engine.eval("c := abs(\"ko\");");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type String, expected \"ko\" to be Double or Long");
    }

    @Test
    public void testExp() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("a := exp(5);");
        assertThat(((Double) context.getAttribute("a"))).isCloseTo(148.41D, Percentage.withPercentage(0.05));
        engine.eval("b := exp(1);");
        assertThat(((Double) context.getAttribute("b"))).isCloseTo(2.72D, Percentage.withPercentage(1));
        engine.eval("c := exp(0);");
        assertThat(context.getAttribute("c")).isEqualTo(1D);
        engine.eval("d := exp(-1);");
        assertThat(((Double) context.getAttribute("d"))).isCloseTo(0.367D, Percentage.withPercentage(1));
        assertThatThrownBy(() -> {
            engine.eval("e := exp(\"ko\");");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type String, expected \"ko\" to be Double or Long");
    }

    @Test
    public void testLn() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("a := ln(148);");
        assertThat(((Double) context.getAttribute("a"))).isCloseTo(5D, Percentage.withPercentage(1));
        engine.eval("b := ln(2.71);");
        assertThat(((Double) context.getAttribute("b"))).isCloseTo(1D, Percentage.withPercentage(1));
        engine.eval("c := ln(1);");
        assertThat(((Double) context.getAttribute("c"))).isCloseTo(0D, Percentage.withPercentage(1));
        engine.eval("d := ln(0.5);");
        assertThat(((Double) context.getAttribute("d"))).isCloseTo(-0.69D, Percentage.withPercentage(1));
        assertThatThrownBy(() -> {
            engine.eval("e := ln(\"ko\");");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type String, expected \"ko\" to be Double or Long");
    }

    @Test
    public void testRound() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("a := round(3.14159, 2);");
        assertThat(context.getAttribute("a")).isEqualTo(3.14D);
        engine.eval("b := round(3.14159, 4);");
        assertThat(context.getAttribute("b")).isEqualTo(3.1416D);
        engine.eval("c := round(12345.6, 0);");
        assertThat(context.getAttribute("c")).isEqualTo(12346D);
        engine.eval("d := round(12345.6);");
        assertThat(context.getAttribute("d")).isEqualTo(12346D);
        engine.eval("e := round(12345.6, -1);");
        assertThat(context.getAttribute("e")).isEqualTo(12350D);
        assertThatThrownBy(() -> {
            engine.eval("f := round(\"ko\", 2);");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type String, expected \"ko\" to be Double or Long");
        assertThatThrownBy(() -> {
            engine.eval("f := round(2.22222, 2.3);");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type Double, expected 2.3 to be Long");
    }

    @Test
    public void testTrunc() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("a := trunc(3.14159, 2);");
        assertThat(context.getAttribute("a")).isEqualTo(3.14D);
        engine.eval("b := trunc(3.14159, 4);");
        assertThat(context.getAttribute("b")).isEqualTo(3.1415D);
        engine.eval("c := trunc(12345.6, 0);");
        assertThat(context.getAttribute("c")).isEqualTo(12345D);
        engine.eval("d := trunc(12345.6);");
        assertThat(context.getAttribute("d")).isEqualTo(12345D);
        engine.eval("e := trunc(12345.6, -1);");
        assertThat(context.getAttribute("e")).isEqualTo(12340D);
        assertThatThrownBy(() -> {
            engine.eval("f := trunc(\"ko\", 2);");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type String, expected \"ko\" to be Double or Long");
        assertThatThrownBy(() -> {
            engine.eval("f := trunc(2.22222, 2.3);");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type Double, expected 2.3 to be Long");
    }

    @Test
    public void testSqrt() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("a := sqrt(25);");
        assertThat(context.getAttribute("a")).isEqualTo(5D);
        engine.eval("c := sqrt(0);");
        assertThat(context.getAttribute("c")).isEqualTo(0D);
        assertThatThrownBy(() -> {
            engine.eval("e := sqrt(-25);");
        }).isInstanceOf(InvalidArgumentException.class)
                .hasMessage("Sqrt operand has to be 0 or positive");
        assertThatThrownBy(() -> {
            engine.eval("e := sqrt(\"ko\");");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type String, expected \"ko\" to be Double or Long");
    }

    @Test
    public void testMod() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("a := mod(5, 2);");
        assertThat(context.getAttribute("a")).isEqualTo(1D);
        engine.eval("b := mod(5, -2);");
        assertThat(context.getAttribute("b")).isEqualTo(-1D);
        engine.eval("c := mod(8, 1);");
        assertThat(context.getAttribute("c")).isEqualTo(0D);
        engine.eval("d := mod(9, 0);");
        assertThat(context.getAttribute("d")).isEqualTo(9D);
        assertThatThrownBy(() -> {
            engine.eval("e := mod(10, \"ko\");");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type String, expected \"ko\" to be Double or Long");
        assertThatThrownBy(() -> {
            engine.eval("f := mod(\"ko\", 10);");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type String, expected \"ko\" to be Double or Long");
    }

    @Test
    public void testPower() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("a := power(5, 2);");
        assertThat(context.getAttribute("a")).isEqualTo(25D);
        engine.eval("b := power(5, 1);");
        assertThat(context.getAttribute("b")).isEqualTo(5D);
        engine.eval("c := power(5, 0);");
        assertThat(context.getAttribute("c")).isEqualTo(1D);
        engine.eval("d := power(5, -1);");
        assertThat(context.getAttribute("d")).isEqualTo(0.2D);
        engine.eval("e := power(-5, 3);");
        assertThat(context.getAttribute("e")).isEqualTo(-125D);
        assertThatThrownBy(() -> {
            engine.eval("f := power(10, \"ko\");");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type String, expected \"ko\" to be Double or Long");
        assertThatThrownBy(() -> {
            engine.eval("e := power(\"ko\", 10);");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type String, expected \"ko\" to be Double or Long");
    }

    @Test
    public void testLog() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("a := log(1024, 2);");
        assertThat(context.getAttribute("a")).isEqualTo(10D);
        engine.eval("b := log(1024, 10);");
        assertThat(((Double) context.getAttribute("b"))).isCloseTo(3.01D, Percentage.withPercentage(0.01));
        assertThatThrownBy(() -> {
            engine.eval("c := log(1024, 0);");
        }).isInstanceOf(InvalidArgumentException.class)
                .hasMessage("Log base has to be greater or equal than 1");
        assertThatThrownBy(() -> {
            engine.eval("d := log(-2, 10);");
        }).isInstanceOf(InvalidArgumentException.class)
                .hasMessage("Log operand has to be positive");
        assertThatThrownBy(() -> {
            engine.eval("e := log(10, \"ko\");");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type String, expected \"ko\" to be Double or Long");
        assertThatThrownBy(() -> {
            engine.eval("f := log(\"ko\", 10);");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type String, expected \"ko\" to be Double or Long");
    }
}
