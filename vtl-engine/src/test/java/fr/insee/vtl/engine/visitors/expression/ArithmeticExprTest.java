package fr.insee.vtl.engine.visitors.expression;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.engine.samples.DatasetSamples;
import fr.insee.vtl.model.Dataset;
import java.util.List;
import java.util.Map;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ArithmeticExprTest {

  private ScriptEngine engine;

  @BeforeEach
  public void setUp() {
    engine = new ScriptEngineManager().getEngineByName("vtl");
  }

  @Test
  public void testNull() throws ScriptException {

    ScriptContext context = engine.getContext();

    List<String> operators = List.of("+", "-", "/", "*");
    List<String> values = List.of("1.1", "1", "cast(null, integer)");

    for (String operator : operators) {
      // Left is null
      for (String value : values) {
        engine.eval("res := cast(null, integer) " + operator + " " + value + " ;");
        assertThat((Boolean) context.getAttribute("res")).isNull();
      }

      // Right is null
      for (String value : values) {
        engine.eval("res := " + value + " " + operator + " cast(null, integer) ;");
        assertThat((Boolean) context.getAttribute("res")).isNull();
      }
    }
  }

  @Test
  public void testArithmeticExpr() throws ScriptException {
    ScriptContext context = engine.getContext();
    engine.eval("mul := 2 * 3;");
    assertThat(context.getAttribute("mul")).isEqualTo(6L);
    engine.eval("mul := 1.5 * 2;");
    assertThat(context.getAttribute("mul")).isEqualTo(3.0);
    engine.eval("mul := 2 * 1.5;");
    assertThat(context.getAttribute("mul")).isEqualTo(3.0);
    engine.eval("mul := 2.0 * 1.5;");
    assertThat(context.getAttribute("mul")).isEqualTo(3.0);

    context.setAttribute("ds1", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    context.setAttribute("ds2", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
    Object res =
        engine.eval("res := round(ds1[keep id, long1, double1] * ds2[keep id, long1, double1]);");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Hadrien", "long1", 1500.0, "double1", 1.0),
            Map.of("id", "Nico", "long1", 400.0, "double1", 27.0),
            Map.of("id", "Franck", "long1", 10000.0, "double1", -1.0));
    //        assertThat(((Dataset)
    // res).getDataStructure().get("long2").getType()).isEqualTo(Long.class);

    engine.eval("div := 6 / 3;");
    assertThat(context.getAttribute("div")).isEqualTo(2.0);
    engine.eval("div := 1 / 0.5;");
    assertThat(context.getAttribute("div")).isEqualTo(2.0);
    engine.eval("div := 2.0 / 1;");
    assertThat(context.getAttribute("div")).isEqualTo(2.0);
    engine.eval("div := 3.0 / 1.5;");
    assertThat(context.getAttribute("div")).isEqualTo(2.0);

    context.setAttribute("ds1", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    context.setAttribute("ds2", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
    res = engine.eval("res := round(ds1[keep id, long1, double1] / ds2[keep id, long1, double1]);");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Hadrien", "long1", 0.0, "double1", 1.0),
            Map.of("id", "Nico", "long1", 1.0, "double1", 6.0),
            Map.of("id", "Franck", "long1", 1.0, "double1", -1.0));
    assertThat(((Dataset) res).getDataStructure().get("long1").getType()).isEqualTo(Double.class);
  }

  @Test
  public void testArithmeticWithVariables() throws ScriptException {
    ScriptContext context = engine.getContext();
    context.setAttribute("two", 2, ScriptContext.ENGINE_SCOPE);
    context.setAttribute("three", 3, ScriptContext.ENGINE_SCOPE);

    engine.eval("mul := two * three;");
    assertThat(context.getAttribute("mul")).isEqualTo(6L);

    context.setAttribute("onePFive", 1.5F, ScriptContext.ENGINE_SCOPE);

    engine.eval("mul := onePFive * two;");
    assertThat(context.getAttribute("mul")).isEqualTo(3.0);

    context.setAttribute("twoLong", 2L, ScriptContext.ENGINE_SCOPE);
    context.setAttribute("onePFiveDouble", 1.5D, ScriptContext.ENGINE_SCOPE);

    engine.eval("mul := twoLong * onePFiveDouble;");
    assertThat(context.getAttribute("mul")).isEqualTo(3.0);
  }
}
