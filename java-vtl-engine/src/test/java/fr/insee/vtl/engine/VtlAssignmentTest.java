package fr.insee.vtl.engine;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.*;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class VtlAssignmentTest {

    private ScriptEngine engine;

    @BeforeEach
    void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    void testVariableExpression() throws ScriptException {
        ScriptContext context = engine.getContext();
        context.setAttribute("foo", 123, ScriptContext.ENGINE_SCOPE);
        engine.eval("bar := foo");
        assertThat(context.getAttribute("bar"))
                .isSameAs(context.getAttribute("foo"));
    }

    @Test
    void testAssignment() throws ScriptException {
        Bindings bindings = engine.createBindings();
        engine.eval("a := 1234;", bindings);
        engine.eval("b := 1234.1234;", bindings);
        engine.eval("c := true;", bindings);
        engine.eval("d := false;", bindings);
        engine.eval("e := \"foo\";", bindings);
        engine.eval("f := null;", bindings);

        assertThat(bindings).containsAllEntriesOf(Map.of(
                "a", 1234L,
                "b", 1234.1234,
                "c", true,
                "d", false,
                "e", "foo"
        ));

        assertThat(bindings.get("f")).isNull();
    }
}