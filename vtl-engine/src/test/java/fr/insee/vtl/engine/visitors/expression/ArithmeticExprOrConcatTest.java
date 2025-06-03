package fr.insee.vtl.engine.visitors.expression;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.engine.samples.DatasetSamples;
import fr.insee.vtl.model.Dataset;
import java.util.Map;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ArithmeticExprOrConcatTest {

  private ScriptEngine engine;

  @BeforeEach
  public void setUp() {
    engine = new ScriptEngineManager().getEngineByName("vtl");
  }

  @Test
  public void testNull() throws ScriptException {
    ScriptContext context = engine.getContext();
    // Plus
    engine.eval("res := 1 + cast(null, integer);");
    assertThat((Long) context.getAttribute("res")).isNull();
    engine.eval("res := cast(null, integer) + 1;");
    assertThat((Long) context.getAttribute("res")).isNull();
    // Minus
    engine.eval("res := 1 - cast(null, integer);");
    assertThat((Long) context.getAttribute("res")).isNull();
    engine.eval("res := cast(null, integer) - 1;");
    assertThat((Long) context.getAttribute("res")).isNull();
    // Concat
    engine.eval("res := \"\" || cast(null, string);");
    assertThat((Boolean) context.getAttribute("res")).isNull();
    engine.eval("res := cast(null, string) || \"\";");
    assertThat((Boolean) context.getAttribute("res")).isNull();
  }

  @Test
  public void testPlus() throws ScriptException {
    ScriptContext context = engine.getContext();
    engine.eval("plus := 2 + 3;");
    assertThat(context.getAttribute("plus")).isEqualTo(5L);
    engine.eval("plus := 2 + 3.0;");
    assertThat(context.getAttribute("plus")).isEqualTo(5.0);
    engine.eval("plus := 2.0 + 3;");
    assertThat(context.getAttribute("plus")).isEqualTo(5.0);

    context.setAttribute("ds1", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    Object res = engine.eval("res := ds1[keep id, long1, long2] + ds1[keep id, long1, long2];");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Toto", "long1", 60L, "long2", 600L),
            Map.of("id", "Hadrien", "long1", 20L, "long2", 2L),
            Map.of("id", "Nico", "long1", 40L, "long2", 500L),
            Map.of("id", "Franck", "long1", 200L, "long2", 4L));
    //        assertThat(((Dataset)
    // res).getDataStructure().get("long2").getType()).isEqualTo(Long.class);
  }

  @Test
  public void testMinus() throws ScriptException {
    ScriptContext context = engine.getContext();
    engine.eval("minus := 3 - 2;");
    assertThat(context.getAttribute("minus")).isEqualTo(1L);
    engine.eval("minus := 3.0 - 2;");
    assertThat(context.getAttribute("minus")).isEqualTo(1.0);
    engine.eval("minus := 3 - 2.0;");
    assertThat(context.getAttribute("minus")).isEqualTo(1.0);

    context.setAttribute("ds1", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    context.setAttribute("ds2", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
    Object res = engine.eval("res := ds2[keep id, long1] - ds1[keep id, long1] + 1;");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Hadrien", "long1", 141L),
            Map.of("id", "Nico", "long1", 1L),
            Map.of("id", "Franck", "long1", 1L));
    assertThat(((Dataset) res).getDataStructure().get("long1").getType()).isEqualTo(Long.class);
  }

  @Test
  public void testConcat() throws ScriptException {
    ScriptContext context = engine.getContext();
    engine.eval("concat := \"3\" || \"ok\";");
    assertThat(context.getAttribute("concat")).isEqualTo("3ok");

    context.setAttribute("ds1", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    context.setAttribute("ds2", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
    Object res = engine.eval("res := ds2[keep id, string1] || \" \" || ds1[keep id, string1];");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Hadrien", "string1", "hadrien hadrien"),
            Map.of("id", "Nico", "string1", "nico nico"),
            Map.of("id", "Franck", "string1", "franck franck"));
    assertThat(((Dataset) res).getDataStructure().get("string1").getType()).isEqualTo(String.class);
  }
}
