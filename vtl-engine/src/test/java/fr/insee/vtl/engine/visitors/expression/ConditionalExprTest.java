package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.model.exceptions.InvalidTypeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ConditionalExprTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testNull() throws ScriptException {
        engine.eval("a := if null then \"true\" else \"false\";");
        assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
        engine.eval("b := if true then null else \"false\";");
        assertThat((Boolean) engine.getContext().getAttribute("b")).isNull();
        engine.eval("c := if false then \"true\" else null;");
        assertThat((Boolean) engine.getContext().getAttribute("c")).isNull();
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
    public void testNvlExpr() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("s := nvl(\"toto\", \"default\");");
        assertThat(context.getAttribute("s")).isEqualTo("toto");
        engine.eval("s := nvl(null, \"default\");");
        assertThat(context.getAttribute("s")).isEqualTo("default");
        assertThatThrownBy(() -> {
            engine.eval("s := nvl(3, \"toto\");");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type String, expected Long");
    }

    @Test
    public void testIfTypeExceptions() {
        assertThatThrownBy(() -> {
            engine.eval("s := if \"\" then 1 else 2;");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type String, expected Boolean");

        assertThatThrownBy(() -> {
            engine.eval("s := if true then \"\" else 2;");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type Long, expected String");
    }
}
