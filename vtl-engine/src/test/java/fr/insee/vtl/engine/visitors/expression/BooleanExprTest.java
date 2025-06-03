package fr.insee.vtl.engine.visitors.expression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import fr.insee.vtl.engine.exceptions.FunctionNotFoundException;
import fr.insee.vtl.engine.samples.DatasetSamples;
import fr.insee.vtl.model.Dataset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BooleanExprTest {

  private ScriptEngine engine;

  @BeforeEach
  public void setUp() {
    engine = new ScriptEngineManager().getEngineByName("vtl");
  }

  @Test
  public void testBooleans() throws ScriptException {
    ScriptContext context = engine.getContext();

    //                  &&      ||      xor
    // true     true    true    true    false
    // true     false   false   true    true
    // false    true    false   true    true
    // false    false   false   false   false

    // null     null    null    null    null
    // null     true    null    true    null
    // null     false   false   null    null
    // true     null    null    true    null
    // false    null    false   null    null
    List<Boolean> a = Arrays.asList(false, false, false, true, true, true, null, null, null);
    List<Boolean> b = Arrays.asList(false, true, null, false, true, null, false, true, null);
    List<Boolean> and = Arrays.asList(false, false, false, false, true, null, false, null, null);
    List<Boolean> or = Arrays.asList(false, true, null, true, true, true, null, true, null);
    List<Boolean> xor = Arrays.asList(false, true, null, true, false, null, null, null, null);

    for (int i = 0; i < a.size(); i++) {
      context.setAttribute("a", a.get(i), ScriptContext.ENGINE_SCOPE);
      context.setAttribute("b", b.get(i), ScriptContext.ENGINE_SCOPE);

      engine.eval(
          "andRes := cast(a, boolean) and cast(b, boolean);"
              + "orRes := cast(a, boolean) or cast(b, boolean);"
              + "xorRes := cast(a, boolean) xor cast(b, boolean);");
      assertThat(context.getAttribute("andRes"))
          .as("%s && %s -> %s", a.get(i), b.get(i), and.get(i))
          .isEqualTo(and.get(i));

      assertThat(context.getAttribute("orRes"))
          .as("%s || %s -> %s", a.get(i), b.get(i), or.get(i))
          .isEqualTo(or.get(i));

      assertThat(context.getAttribute("xorRes"))
          .as("%s ^ %s -> %s", a.get(i), b.get(i), xor.get(i))
          .isEqualTo(xor.get(i));
    }
  }

  @Test
  public void testBooleanTypeExceptions() {
    assertThatThrownBy(
            () -> {
              engine.eval("s := 1 and 2;");
            })
        .isInstanceOf(FunctionNotFoundException.class)
        .hasMessage("function 'and(Long, Long)' not found");

    assertThatThrownBy(
            () -> {
              engine.eval("s := true or 2;");
            })
        .isInstanceOf(FunctionNotFoundException.class)
        .hasMessage("function 'or(Boolean, Long)' not found");
  }

  @Test
  public void testOnDatasets() throws ScriptException {
    ScriptContext context = engine.getContext();

    context.setAttribute("ds_1", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    context.setAttribute("ds_2", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    engine.eval(
        "ds1 := ds_1[keep id, bool2][rename bool2 to bool1]; "
            + "ds2 := ds_2[keep id, bool1]; "
            + "andDs := ds1 and ds2; "
            + "orDs := ds1 or ds2; "
            + "xorDs := ds1 xor ds2; ");
    Dataset and = (Dataset) context.getAttribute("andDs");
    assertThat(and.getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Toto", "bool_var", false),
            Map.of("id", "Hadrien", "bool_var", true),
            Map.of("id", "Nico", "bool_var", false),
            Map.of("id", "Franck", "bool_var", false));
    Dataset or = (Dataset) context.getAttribute("orDs");
    assertThat(or.getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Toto", "bool_var", true),
            Map.of("id", "Hadrien", "bool_var", true),
            Map.of("id", "Nico", "bool_var", true),
            Map.of("id", "Franck", "bool_var", false));
    Dataset xor = (Dataset) context.getAttribute("xorDs");
    assertThat(xor.getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Toto", "bool_var", true),
            Map.of("id", "Hadrien", "bool_var", false),
            Map.of("id", "Nico", "bool_var", true),
            Map.of("id", "Franck", "bool_var", false));
  }
}
