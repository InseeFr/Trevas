package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.model.exceptions.InvalidTypeException;
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
    public void testNull() throws ScriptException {
        // Trim
        engine.eval("a := trim(null);");
        assertThat((String) engine.getContext().getAttribute("a")).isNull();
        // Ltrim
        engine.eval("a := ltrim(null);");
        assertThat((String) engine.getContext().getAttribute("a")).isNull();
        // Rtrim
        engine.eval("a := rtrim(null);");
        assertThat((String) engine.getContext().getAttribute("a")).isNull();
        // Upper
        engine.eval("a := upper(null);");
        assertThat((String) engine.getContext().getAttribute("a")).isNull();
        // Lower
        engine.eval("a := lower(null);");
        assertThat((String) engine.getContext().getAttribute("a")).isNull();
        // Length
        engine.eval("a := length(null);");
        assertThat((Long) engine.getContext().getAttribute("a")).isNull();
        // Substr
        engine.eval("a := substr(null);");
        assertThat((String) engine.getContext().getAttribute("a")).isNull();
        engine.eval("b := substr(\"ok\", null, 2);");
        assertThat((String) engine.getContext().getAttribute("b")).isEqualTo("ok");
        engine.eval("c := substr(\"ok\", 1, null);");
        assertThat((String) engine.getContext().getAttribute("c")).isEqualTo("ok");
        // Replace
        engine.eval("a := replace(null, \"ooo\", \"ttt\");");
        assertThat((String) engine.getContext().getAttribute("a")).isNull();
        engine.eval("b := replace(\"ok\", null, \"ttt\");");
        assertThat((String) engine.getContext().getAttribute("b")).isNull();
        engine.eval("c := replace(\"ok\", \"ooo\", null);");
        assertThat((String) engine.getContext().getAttribute("c")).isEqualTo("ok");
        // Instr
        engine.eval("a := instr(null, \"ooo\", 1, 2);");
        assertThat((Long) engine.getContext().getAttribute("a")).isNull();
        engine.eval("b := instr(\"ok\", null, 1, 2);");
        assertThat((Long) engine.getContext().getAttribute("b")).isNull();
        engine.eval("c := instr(\"ok\", \"ooo\", null, 2);");
        assertThat((Long) engine.getContext().getAttribute("c")).isEqualTo(0L);
        engine.eval("d := instr(\"ok\", \"ooo\", 1, null);");
        assertThat((Long) engine.getContext().getAttribute("d")).isEqualTo(0L);
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
        engine.eval("s1 := substr(\"abcdefghijklmnopqrstuvwxyz\");");
        assertThat(context.getAttribute("s1")).isEqualTo("abcdefghijklmnopqrstuvwxyz");
        engine.eval("s1 := substr(\"abcdefghijklmnopqrstuvwxyz\", 2);");
        assertThat(context.getAttribute("s1")).isEqualTo("bcdefghijklmnopqrstuvwxyz");
        engine.eval("s1 := substr(\"abcdefghijklmnopqrstuvwxyz\", 5, 10);");
        assertThat(context.getAttribute("s1")).isEqualTo("efghijklmn");
        engine.eval("s1 := substr(\"abcdefghijklmnopqrstuvwxyz\", 25, 10);");
        assertThat(context.getAttribute("s1")).isEqualTo("yz");
        engine.eval("s1 := substr(\"abcdefghijklmnopqrstuvwxyz\", 30, 10);");
        assertThat(context.getAttribute("s1")).isEqualTo("");
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
                .hasMessage("invalid type Long, expected String");
        assertThatThrownBy(() -> {
            engine.eval("re1 := replace(\"abc\",\"ok\",true);");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type Boolean, expected String");
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
        engine.eval("i4 := instr (\"abcdecfrxcwsd\", \"c\", 5, 3);");
        assertThat(context.getAttribute("i4")).isEqualTo(0L);

        assertThatThrownBy(() -> {
            engine.eval("re1 := instr(\"abc\",1);");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type Long, expected String");
        assertThatThrownBy(() -> {
            engine.eval("re2 := instr(\"abc\", \"c\", \"ko\");");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type String, expected Long");
        assertThatThrownBy(() -> {
            engine.eval("re2 := instr(\"abc\", \"c\", 1, \"ko\");");
        }).isInstanceOf(InvalidTypeException.class)
                .hasMessage("invalid type String, expected Long");
    }
}
