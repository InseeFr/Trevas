package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.exceptions.InvalidTypeException;
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
    public void testMod() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("a := mod(5, 2);");
        assertThat(context.getAttribute("a")).isEqualTo(1D);
//        engine.eval("b := mod(5, -2);");
//        assertThat(context.getAttribute("b")).isEqualTo(-1D);
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
//        assertThatThrownBy(() -> {
//            engine.eval("b := log(1024, 1);");
//        }).isInstanceOf(InvalidTypeException.class)
//                .hasMessage("invalid type String, expected \"ko\" to be Double or Long");
//        assertThatThrownBy(() -> {
//            engine.eval("c := log(-2, 10);");
//        }).isInstanceOf(InvalidTypeException.class)
//                .hasMessage("invalid type String, expected \"ko\" to be Double or Long");
        assertThatThrownBy(() -> {
            engine.eval("d := log(10, \"ko\");");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type String, expected \"ko\" to be Double or Long");
        assertThatThrownBy(() -> {
            engine.eval("e := log(\"ko\", 10);");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type String, expected \"ko\" to be Double or Long");
    }
}
