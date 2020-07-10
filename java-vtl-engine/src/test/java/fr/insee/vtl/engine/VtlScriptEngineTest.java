package fr.insee.vtl.engine;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class VtlScriptEngineTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    void testExceptions() {
        assertThatThrownBy(() -> {
            engine.eval("var := \"string\" + 42");
        }).isInstanceOf(InvalidTypeException.class);

        assertThatThrownBy(() -> {
            engine.eval("var := undefinedVariable + 42");
        }).isInstanceOf(InvalidTypeException.class);
    }
}