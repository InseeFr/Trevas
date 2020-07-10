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

public class IfExprTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testIfExpr() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("s := if true then \"true\" else \"false\";");
        assertThat(context.getAttribute("s")).isEqualTo("true");
        engine.eval("l := if false then 1 else 0;");
        assertThat(context.getAttribute("l")).isEqualTo(0L);
    }

    @Test
    void testIfTypeExceptions() {
        assertThatThrownBy(() -> {
            engine.eval("s := if \"\" then 1 else 2;");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type String, expected \"\" to be Boolean");

        assertThatThrownBy(() -> {
            engine.eval("s := if true then \"\" else 2;");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type Long, expected 2 to be String");
    }
}
