package fr.insee.vtl.engine.visitors.expression.functions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import static org.assertj.core.api.Assertions.assertThat;

public class ComparisonFunctionsTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testCharsetMatchAtom() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("t := match_characters(\"test\", \"(.*)(es)(.*)?\");");
        assertThat((Boolean) context.getAttribute("t")).isTrue();
        engine.eval("t := match_characters(\"test\", \"(.*)(aaaaa)(.*)?\");");
        assertThat((Boolean) context.getAttribute("t")).isFalse();
    }
}
