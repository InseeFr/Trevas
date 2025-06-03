package fr.insee.vtl.engine.visitors.expression;

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

public class UnaryExprTest {

  private ScriptEngine engine;

  @BeforeEach
  public void setUp() {
    engine = new ScriptEngineManager().getEngineByName("vtl");
  }

  @Test
  public void testNull() throws ScriptException {
    ScriptContext context = engine.getContext();
    engine.eval("res := - cast(null, number);");
    assertThat(context.getAttribute("res")).isNull();
    engine.eval("res := + cast(null, number);");
    assertThat(context.getAttribute("res")).isNull();
    engine.eval("res := not cast(null, boolean);");
    assertThat(context.getAttribute("res")).isNull();
  }

  @Test
  public void testUnaryExpr() throws ScriptException {
    ScriptContext context = engine.getContext();

    engine.eval("plus := +1;");
    assertThat(context.getAttribute("plus")).isEqualTo(1L);
    engine.eval("plus := + 1.5;");
    assertThat(context.getAttribute("plus")).isEqualTo(1.5D);

    context.setAttribute("ds", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
    Object res = engine.eval("res := + ds[keep id, long1, double1];");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Hadrien", "long1", 150L, "double1", 1.1D),
            Map.of("id", "Nico", "long1", 20L, "double1", 2.2D),
            Map.of("id", "Franck", "long1", 100L, "double1", -1.21D));
    assertThat(((Dataset) res).getDataStructure().get("long1").getType()).isEqualTo(Long.class);

    engine.eval("plus := -1;");
    assertThat(context.getAttribute("plus")).isEqualTo(-1L);
    engine.eval("plus := - 1.5;");
    assertThat(context.getAttribute("plus")).isEqualTo(-1.5D);

    context.setAttribute("ds", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
    res = engine.eval("res := - ds[keep id, long1, double1];");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Hadrien", "long1", -150L, "double1", -1.1D),
            Map.of("id", "Nico", "long1", -20L, "double1", -2.2D),
            Map.of("id", "Franck", "long1", -100L, "double1", 1.21D));
    assertThat(((Dataset) res).getDataStructure().get("long1").getType()).isEqualTo(Long.class);

    assertThatThrownBy(
            () -> {
              engine.eval("plus := + \"ko\";");
            })
        .isInstanceOf(FunctionNotFoundException.class)
        .hasMessage("function 'plus(String)' not found");
    assertThatThrownBy(
            () -> {
              engine.eval("minus := - \"ko\";");
            })
        .isInstanceOf(FunctionNotFoundException.class)
        .hasMessage("function 'minus(String)' not found");
  }
}
