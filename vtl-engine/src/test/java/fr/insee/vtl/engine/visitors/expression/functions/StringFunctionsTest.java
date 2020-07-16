package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.exceptions.InvalidTypeException;
import fr.insee.vtl.engine.exceptions.UnsupportedTypeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class StringFunctionsTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testUnaryStringFunction() throws ScriptException {
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

    @Test
    public void testSubstrAtom() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("s1 := substr(\"abcde\");");
        assertThat(context.getAttribute("s1")).isEqualTo("abcde");
        engine.eval("s1 := substr(\"abcde\",1);");
        assertThat(context.getAttribute("s1")).isEqualTo("bcde");
        engine.eval("s1 := substr(\"abcde\",1,3);");
        assertThat(context.getAttribute("s1")).isEqualTo("bc");

        assertThatThrownBy(() -> {
            engine.eval("se1 := substr(\"abc\",1,2,3);");
        }).isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("too many args (3) for: substr(\"abc\",1,2,3)");
        assertThatThrownBy(() -> {
            engine.eval("se2 := substr(\"abc\",1,2,3,4,5,6);");
        }).isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("too many args (6) for: substr(\"abc\",1,2,3,4,5,6)");
    }

    @Test
    public void testReplaceAtom() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("r1 := replace(\"abcde\", \"abc\", \"ABC\");");
        assertThat(context.getAttribute("r1")).isEqualTo("ABCde");
        engine.eval("r2 := replace(\"abcde\", \"abc\");");
        assertThat(context.getAttribute("r2")).isEqualTo("de");

        assertThatThrownBy(() -> {
            engine.eval("re1 := replace(\"abc\",1,\"ok\");");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type Long, expected 1 to be String");
        assertThatThrownBy(() -> {
            engine.eval("re1 := replace(\"abc\",\"ok\",true);");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type Boolean, expected true to be String");
    }

    @Test
    public void testInstrAtom() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("i1 := instr(\"abcde\", \"c\");");
        assertThat(context.getAttribute("i1")).isEqualTo(3L);
        engine.eval("i2 := instr(\"abcde\", \"c\", 4);");
        assertThat(context.getAttribute("i2")).isEqualTo(0L);
        engine.eval("i3 := instr (\"abcdecfrxcwsd\", \"c\", 0, 3);");
        assertThat(context.getAttribute("i3")).isEqualTo(10L);
        engine.eval("i4 := instr (\"abcdecfrxcwsd\", \"c\", 5, 3)");
        assertThat(context.getAttribute("i4")).isEqualTo(0L);

        assertThatThrownBy(() -> {
            engine.eval("re1 := instr(\"abc\",1);");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type Long, expected 1 to be String");
        assertThatThrownBy(() -> {
            engine.eval("re2 := instr(\"abc\", \"c\", \"ko\");");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type String, expected \"ko\" to be Long");
        assertThatThrownBy(() -> {
            engine.eval("re2 := instr(\"abc\", \"c\", 1, \"ko\");");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type String, expected \"ko\" to be Long");
    }
}
