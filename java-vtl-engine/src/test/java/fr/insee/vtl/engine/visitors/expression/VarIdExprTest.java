package fr.insee.vtl.engine.visitors.expression;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import static org.assertj.core.api.Assertions.assertThat;

public class VarIdExprTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testVariableExpression() throws ScriptException {
        ScriptContext context = engine.getContext();

        context.setAttribute("foo", 123L, ScriptContext.ENGINE_SCOPE);
        engine.eval("bar := foo;");
        assertThat(context.getAttribute("bar")).isSameAs(context.getAttribute("foo"));

        context.setAttribute("foo", 123D, ScriptContext.ENGINE_SCOPE);
        engine.eval("bar := foo;");
        assertThat(context.getAttribute("bar")).isSameAs(context.getAttribute("foo"));


        context.setAttribute("foo", null, ScriptContext.ENGINE_SCOPE);
        engine.eval("bar := foo;");

        assertThat(context.getAttribute("bar")).isSameAs(context.getAttribute("foo"));

    }

    @Test
    void testAutoCastVariables() throws ScriptException {
        ScriptContext context = engine.getContext();

        context.setAttribute("anInt", Integer.valueOf(123), ScriptContext.ENGINE_SCOPE);
        context.setAttribute("aFloat", Float.valueOf(1.5F), ScriptContext.ENGINE_SCOPE);

        engine.eval("theInt := anInt; theFloat := aFloat;");

        assertThat(context.getAttribute("theInt"))
                .isEqualTo(123L);
        assertThat(context.getAttribute("theFloat"))
                .isEqualTo(1.5D);
    }
}
