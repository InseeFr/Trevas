package fr.insee.vtl.engine.visitors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.*;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class AssignmentTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testAssignment() throws ScriptException {
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