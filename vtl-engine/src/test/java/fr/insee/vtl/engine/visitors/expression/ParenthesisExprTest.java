package fr.insee.vtl.engine.visitors.expression;

import static org.assertj.core.api.Assertions.assertThat;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ParenthesisExprTest {

  private ScriptEngine engine;

  @BeforeEach
  public void setUp() {
    engine = new ScriptEngineManager().getEngineByName("vtl");
  }

  @Test
  public void parenthesisExpr() throws ScriptException {
    ScriptContext context = engine.getContext();
    engine.eval("three := (3);");
    engine.eval("trueBoolean := (true);");
    assertThat(context.getAttribute("three")).isEqualTo(3L);
    assertThat((Boolean) context.getAttribute("trueBoolean")).isTrue();
  }

  @Test
  public void testPrecedence() throws ScriptException {
    ScriptContext context = engine.getContext();
    engine.eval("withoutParenthesis := 1 + 2 * 3;");
    engine.eval("withParenthesis := (1 + 2) * 3;");
    assertThat(context.getAttribute("withoutParenthesis")).isEqualTo(7L);
    assertThat(context.getAttribute("withParenthesis")).isEqualTo(9L);
  }
}
