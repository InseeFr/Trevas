package fr.insee.vtl.engine.visitors.expression.functions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import fr.insee.vtl.engine.exceptions.FunctionNotFoundException;
import fr.insee.vtl.engine.samples.DatasetSamples;
import fr.insee.vtl.model.Dataset;
import java.util.Map;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StringFunctionsTest {

  private ScriptEngine engine;

  @BeforeEach
  public void setUp() {
    engine = new ScriptEngineManager().getEngineByName("vtl");
  }

  @Test
  public void testNull() throws ScriptException {
    // Trim
    engine.eval("a := trim(cast(null, string));");
    assertThat((String) engine.getContext().getAttribute("a")).isNull();
    // Ltrim
    engine.eval("a := ltrim(cast(null, string));");
    assertThat((String) engine.getContext().getAttribute("a")).isNull();
    // Rtrim
    engine.eval("a := rtrim(cast(null, string));");
    assertThat((String) engine.getContext().getAttribute("a")).isNull();
    // Upper
    engine.eval("a := upper(cast(null, string));");
    assertThat((String) engine.getContext().getAttribute("a")).isNull();
    // Lower
    engine.eval("a := lower(cast(null, string));");
    assertThat((String) engine.getContext().getAttribute("a")).isNull();
    // Length
    engine.eval("a := length(cast(null, string));");
    assertThat((Long) engine.getContext().getAttribute("a")).isNull();
    // Substr
    engine.eval("a := substr(cast(null, string));");
    assertThat((String) engine.getContext().getAttribute("a")).isNull();
    engine.eval("b := substr(\"ok\", cast(null, integer), 2);");
    assertThat((String) engine.getContext().getAttribute("b")).isEqualTo("ok");
    engine.eval("c := substr(\"ok\", 1, cast(null, integer));");
    assertThat((String) engine.getContext().getAttribute("c")).isEqualTo("ok");
    // Replace
    engine.eval("a := replace(cast(null, string), \"ooo\", \"ttt\");");
    assertThat((String) engine.getContext().getAttribute("a")).isNull();
    engine.eval("b := replace(\"ok\", cast(null, string), \"ttt\");");
    assertThat((String) engine.getContext().getAttribute("b")).isNull();
    engine.eval("c := replace(\"ok\", \"ooo\", cast(null, string));");
    assertThat((String) engine.getContext().getAttribute("c")).isEqualTo("ok");
    // Instr
    engine.eval("a := instr(cast(null, string), \"ooo\", 1, 2);");
    assertThat((Long) engine.getContext().getAttribute("a")).isNull();
    engine.eval("b := instr(\"ok\", cast(null, string), 1, 2);");
    assertThat((Long) engine.getContext().getAttribute("b")).isNull();
    engine.eval("c := instr(\"ok\", \"ooo\", cast(null, integer), 2);");
    assertThat((Long) engine.getContext().getAttribute("c")).isZero();
    engine.eval("d := instr(\"ok\", \"ooo\", 1, cast(null, integer));");
    assertThat((Long) engine.getContext().getAttribute("d")).isZero();
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

    context.setAttribute("ds", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    engine.eval(
        "dsTrim := trim(ds[keep id, string1]); "
            + "dsLTrim := ltrim(ds[keep id, string1]); "
            + "dsRTrim := rtrim(ds[keep id, string1]); "
            + "dsUpper := upper(ds[keep id, string1]); "
            + "dsLower := lower(ds[keep id, string1]); "
            + "dsLen := length(ds[keep id, string1]);");
    assertThat(((Dataset) context.getAttribute("dsTrim")).getDataAsMap().get(0))
        .isEqualTo(Map.of("id", "Toto", "string1", "toto"));
    assertThat(((Dataset) context.getAttribute("dsLTrim")).getDataAsMap().get(0))
        .isEqualTo(Map.of("id", "Toto", "string1", "toto"));
    assertThat(((Dataset) context.getAttribute("dsRTrim")).getDataAsMap().get(0))
        .isEqualTo(Map.of("id", "Toto", "string1", "toto"));
    assertThat(((Dataset) context.getAttribute("dsUpper")).getDataAsMap().get(0))
        .isEqualTo(Map.of("id", "Toto", "string1", "TOTO"));
    assertThat(((Dataset) context.getAttribute("dsLower")).getDataAsMap().get(0))
        .isEqualTo(Map.of("id", "Toto", "string1", "toto"));
    assertThat(
            ((Dataset) context.getAttribute("dsLower")).getDataStructure().get("string1").getType())
        .isEqualTo(String.class);
    assertThat(((Dataset) context.getAttribute("dsLen")).getDataAsMap().get(0))
        .isEqualTo(Map.of("id", "Toto", "string1", 4L));
    assertThat(
            ((Dataset) context.getAttribute("dsLen")).getDataStructure().get("string1").getType())
        .isEqualTo(Long.class);
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

    context.setAttribute("ds", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    Object res = engine.eval("res := substr(ds[keep id, string1, string2], 2, 4);");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Toto", "string1", "oto", "string2", ""),
            Map.of("id", "Hadrien", "string1", "adri", "string2", ""),
            Map.of("id", "Nico", "string1", "ico", "string2", ""),
            Map.of("id", "Franck", "string1", "ranc", "string2", ""));
    assertThat(((Dataset) res).getDataStructure().get("string1").getType()).isEqualTo(String.class);
  }

  @Test
  public void testReplaceAtom() throws ScriptException {
    ScriptContext context = engine.getContext();
    engine.eval("r1 := replace(\"abcde\", \"abc\", \"ABC\");");
    assertThat(context.getAttribute("r1")).isEqualTo("ABCde");
    engine.eval("r2 := replace(\"abcde\", \"abc\");");
    assertThat(context.getAttribute("r2")).isEqualTo("de");

    context.setAttribute("ds", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    Object res = engine.eval("res := replace(ds[keep id, string1, string2], \"o\", \"O\");");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Toto", "string1", "tOtO", "string2", "t"),
            Map.of("id", "Hadrien", "string1", "hadrien", "string2", "k"),
            Map.of("id", "Nico", "string1", "nicO", "string2", "l"),
            Map.of("id", "Franck", "string1", "franck", "string2", "c"));
    assertThat(((Dataset) res).getDataStructure().get("string1").getType()).isEqualTo(String.class);

    assertThatThrownBy(
            () -> {
              engine.eval("re1 := replace(\"abc\",1,\"ok\");");
            })
        .isInstanceOf(FunctionNotFoundException.class)
        .hasMessage("function 'replace(String, Long, String)' not found");
    assertThatThrownBy(
            () -> {
              engine.eval("re1 := replace(\"abc\",\"ok\",true);");
            })
        .isInstanceOf(FunctionNotFoundException.class)
        .hasMessage("function 'replace(String, String, Boolean)' not found");
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

    context.setAttribute("ds", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    Object res = engine.eval("res := instr(ds[keep id, string1, string2], \"o\", 0, 2);");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Toto", "string1", 4L, "string2", 0L),
            Map.of("id", "Hadrien", "string1", 0L, "string2", 0L),
            Map.of("id", "Nico", "string1", 0L, "string2", 0L),
            Map.of("id", "Franck", "string1", 0L, "string2", 0L));
    assertThat(((Dataset) res).getDataStructure().get("string1").getType()).isEqualTo(Long.class);

    assertThatThrownBy(
            () -> {
              engine.eval("re1 := instr(\"abc\",1);");
            })
        .isInstanceOf(FunctionNotFoundException.class)
        .hasMessage("function 'instr(String, Long, Long, Long)' not found");
    assertThatThrownBy(
            () -> {
              engine.eval("re2 := instr(\"abc\", \"c\", \"ko\");");
            })
        .isInstanceOf(FunctionNotFoundException.class)
        .hasMessage("function 'instr(String, String, String, Long)' not found");
    assertThatThrownBy(
            () -> {
              engine.eval("re2 := instr(\"abc\", \"c\", 1, \"ko\");");
            })
        .isInstanceOf(FunctionNotFoundException.class)
        .hasMessage("function 'instr(String, String, Long, String)' not found");
  }
}
