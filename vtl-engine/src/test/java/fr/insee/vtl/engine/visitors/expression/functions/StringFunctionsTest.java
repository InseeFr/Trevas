package fr.insee.vtl.engine.visitors.expression.functions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import static org.assertj.core.api.Assertions.assertThat;

public class StringFunctionsTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testStringFunctions() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("trimValue := trim(\"  abc  \");");
        assertThat(context.getAttribute("trimValue")).isEqualTo("abc");
        engine.eval("ltrimValue := ltrim(\"  abc  \");");
        assertThat(context.getAttribute("ltrimValue")).isEqualTo("abc  ");
        engine.eval("rtrimValue := rtrim(\"  abc  \");");
        assertThat(context.getAttribute("rtrimValue")).isEqualTo("  abc");
        engine.eval("upperValue := upper(\"Abc\");");
        assertThat(context.getAttribute("upperValue")).isEqualTo("ABC");
        engine.eval("lowerValue := lower(\"Abc\");");
        assertThat(context.getAttribute("lowerValue")).isEqualTo("abc");
        engine.eval("lengthValue := length(\"abc\");");
        assertThat(context.getAttribute("lengthValue")).isEqualTo(3L);
    }
}
