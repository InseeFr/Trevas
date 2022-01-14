package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.exceptions.ConflictingTypesException;
import fr.insee.vtl.engine.exceptions.InvalidTypeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ComparisonFunctionsTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testNull() throws  ScriptException {
        // Between
        engine.eval("a := between(null, 1, 100);");
        assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
        engine.eval("b := between(10, null, 100);");
        assertThat((Boolean) engine.getContext().getAttribute("b")).isNull();
        engine.eval("c := between(10, 1, null);");
        assertThat((Boolean) engine.getContext().getAttribute("c")).isNull();
        // CharsetMatch
        engine.eval("a := match_characters(\"ko\", null);");
        assertThat((Boolean) engine.getContext().getAttribute("a")).isNull();
        engine.eval("b := match_characters(null, \"test\");");
        assertThat((Boolean) engine.getContext().getAttribute("b")).isNull();
    }

    @Test
    public void testBetweenAtom() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("b := between(10, 1,100);");
        assertThat((Boolean) context.getAttribute("b")).isTrue();
        engine.eval("b := between(10, 20,100);");
        assertThat((Boolean) context.getAttribute("b")).isFalse();
        engine.eval("b := between(10.5, 20,100);");
        assertThat((Boolean) context.getAttribute("b")).isFalse();
        assertThatThrownBy(() -> {
            engine.eval("b := between(10.5, \"ko\", true);");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type Double, expected 10.5 to be String");
    }

    @Test
    public void testCharsetMatchAtom() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("t := match_characters(\"test\", \"(.*)(es)(.*)?\");");
        assertThat((Boolean) context.getAttribute("t")).isTrue();
        engine.eval("t := match_characters(\"test\", \"tes.\");");
        assertThat((Boolean) context.getAttribute("t")).isTrue();
        engine.eval("t := match_characters(\"test\", \"tes\");");
        assertThat((Boolean) context.getAttribute("t")).isFalse();
        engine.eval("t := match_characters(\"test\", \"(.*)(aaaaa)(.*)?\");");
        assertThat((Boolean) context.getAttribute("t")).isFalse();
        assertThatThrownBy(() -> {
            engine.eval("t := match_characters(\"test\", true);");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type Boolean, expected true to be String");
        assertThatThrownBy(() -> {
            engine.eval("t := match_characters(10.5, \"pattern\");");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type Double, expected 10.5 to be String");
    }

    @Test
    public void testIsNullAtom() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("n := isnull(null);");
        assertThat((Boolean) context.getAttribute("n")).isTrue();
        engine.eval("n := isnull(\"null\");");
        assertThat((Boolean) context.getAttribute("n")).isFalse();
    }

}
