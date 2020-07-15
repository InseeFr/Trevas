package fr.insee.vtl.engine.visitors.expression;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import static org.assertj.core.api.Assertions.assertThat;

public class ParenthesisExprTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void parenthesisExpr() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("three := (3);");
        engine.eval("trueBoolean := (true);");
        // TODO: More test with arithmetic
        assertThat(context.getAttribute("three")).isEqualTo(3L);
        assertThat(context.getAttribute("trueBoolean")).isEqualTo(true);
    }
}
