package fr.insee.vtl.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.hervian.reflection.Fun;
import fr.insee.vtl.engine.exceptions.FunctionNotFoundException;
import fr.insee.vtl.engine.exceptions.UndefinedVariableException;
import fr.insee.vtl.engine.exceptions.VtlSyntaxException;
import fr.insee.vtl.model.*;
import fr.insee.vtl.model.exceptions.InvalidTypeException;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class VtlScriptEngineTest {

  private ScriptEngine engine;

  public static <T extends Throwable> Condition<T> atPosition(
      Integer line, Integer startColumn, Integer endColumn) {
    return atPosition(line, line, startColumn, endColumn);
  }

  public static <T extends Throwable> Condition<T> atPosition(
      Integer startLine, Integer endLine, Integer startColumn, Integer endColumn) {
    return new Condition<>(
        throwable -> {
          var scriptException = (VtlScriptException) throwable;
          var position = scriptException.getPosition();
          return position.startLine.equals(startLine)
              && position.endLine.equals(endLine)
              && position.startColumn.equals(startColumn)
              && position.endColumn.equals(endColumn);
        },
        "at position <%d:%d-%d:%d>",
        startLine,
        endLine,
        startColumn,
        endColumn);
  }

  public static <T extends Comparable<T>> Boolean test(T left, T right) {
    return true;
  }

  @BeforeEach
  public void setUp() {
    engine = new ScriptEngineManager().getEngineByName("vtl");
  }

  @Test
  public void testProcessingEngines() {
    VtlScriptEngine vtlScriptEngine = (VtlScriptEngine) engine;
    ProcessingEngine processingEngines = vtlScriptEngine.getProcessingEngine();
    assertThat(processingEngines).isNotNull();
  }

  @Test
  public void testNonPersistentAssignment() throws ScriptException {
    // Non persistent
    VtlScriptEngine engine = (VtlScriptEngine) this.engine;
    ScriptContext context = engine.getContext();
    context.setAttribute("a", 1L, ScriptContext.ENGINE_SCOPE);
    engine.eval("b := a;");
    assertThat(context.getBindings(ScriptContext.ENGINE_SCOPE)).containsKey("b");
    assertThat((Long) context.getAttribute("b")).isEqualTo(1L);
    assertThat(context.getBindings(ScriptContext.GLOBAL_SCOPE)).doesNotContainKey("b");
  }

  @Test
  public void testPersistentAssignmentWithScalar() {
    VtlScriptEngine engine = (VtlScriptEngine) this.engine;
    ScriptContext context = engine.getContext();
    context.setAttribute("a", 1L, ScriptContext.ENGINE_SCOPE);
    assertThatThrownBy(() -> engine.eval("b <- a;"))
        .isInstanceOf(InvalidTypeException.class)
        .is(atPosition(0, 0, 0, 6))
        .hasMessage("invalid type Long, expected Dataset");
  }

  @Test
  public void testPersistentAssignmentWithDs() throws ScriptException {
    VtlScriptEngine engine = (VtlScriptEngine) this.engine;
    ScriptContext context = engine.getContext();

    var ds = new InMemoryDataset(List.of(), Map.of());
    context.setAttribute("ds", ds, ScriptContext.ENGINE_SCOPE);
    engine.eval("pds <- ds;");

    assertThat(context.getBindings(ScriptContext.ENGINE_SCOPE)).containsKey("pds");
    assertThat(context.getBindings(ScriptContext.ENGINE_SCOPE).get("pds"))
        .isInstanceOf(PersistentDataset.class);

    assertThat(((PersistentDataset) context.getAttribute("pds")).getDataStructure())
        .isEqualTo(ds.getDataStructure());
    assertThat(((PersistentDataset) context.getAttribute("pds")).getDelegate()).isSameAs(ds);
    assertThat(context.getBindings(ScriptContext.GLOBAL_SCOPE)).doesNotContainKey("b");
  }

  @Test
  public void testFunctionsExpression() throws NoSuchMethodException, ScriptException {

    InMemoryDataset ds =
        new InMemoryDataset(
            List.of(
                new Structured.Component("name", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("age", Double.class, Dataset.Role.MEASURE),
                new Structured.Component("weight", Double.class, Dataset.Role.MEASURE)),
            Arrays.asList("Toto", null, 12.34),
            Arrays.asList("Hadrien", 12.34, 12.34),
            Arrays.asList("Nico", 12.34, 12.34));

    var ceilMethod = MathFunctions.class.getMethod("myCustomCeil", Double.class);

    VtlScriptEngine engine = (VtlScriptEngine) this.engine;
    engine.registerMethod("myCustomCeil", ceilMethod);
    ScriptContext context = engine.getContext();
    context.setAttribute("ds", ds, ScriptContext.ENGINE_SCOPE);

    Object res1 = engine.eval("res := myCustomCeil(ds);");
    assertThat(res1).isNotNull();
  }

  @Test
  void testMultipleParameterFunctionExpressions() throws ScriptException {
    var ds1 =
        new InMemoryDataset(
            List.of(
                new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("m1", Double.class, Dataset.Role.MEASURE)),
            Arrays.asList("Toto", 1.234567890),
            Arrays.asList("Hadrien", 2.345678901),
            Arrays.asList("Nico", 4.567890123),
            Arrays.asList("NotHere", 9.87654321));
    var ds2 =
        new InMemoryDataset(
            List.of(
                new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("m1", Long.class, Dataset.Role.MEASURE)),
            Arrays.asList("Toto", 4L),
            Arrays.asList("Hadrien", 2L),
            Arrays.asList("Nico", 1L),
            Arrays.asList("NotThere", 9.87654321));
    engine.getContext().setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);
    engine.getContext().setAttribute("ds2", ds2, ScriptContext.ENGINE_SCOPE);
    engine.eval("res := round(ds1#m1, ds2#m1);");

    assertThat(((Dataset) engine.get("res")).getDataAsList())
        .containsExactly(List.of("Toto", 1.2346), List.of("Hadrien", 2.35), List.of("Nico", 4.6));
  }

  @Test
  void testMultipleParameterFunctionExpressionsAndConstants() throws ScriptException {
    var ds1 =
        new InMemoryDataset(
            List.of(
                new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("m1", Long.class, Dataset.Role.MEASURE)),
            Arrays.asList("Toto", 2L),
            Arrays.asList("Hadrien", 4L),
            Arrays.asList("Nico", 8L));
    engine.getContext().setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);
    engine.eval("res := round(0.123456789, ds1#m1);");

    assertThat(((Dataset) engine.get("res")).getDataAsList())
        .containsExactly(
            List.of("Toto", 0.12), List.of("Hadrien", 0.1235), List.of("Nico", 0.12345679));
  }

  @Test
  void testFunctionExpressionsWrongType() throws ScriptException {
    var ds1 =
        new InMemoryDataset(
            List.of(
                new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("m1", Long.class, Dataset.Role.MEASURE)),
            Arrays.asList("Toto", 2),
            Arrays.asList("Hadrien", 4),
            Arrays.asList("Nico", 8));
    engine.getContext().setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);
    assertThatThrownBy(
            () -> {
              engine.eval("res := round(0.123456789, ds1#m1);");
            })
        .isInstanceOf(VtlScriptException.class);
  }

  @Disabled
  @Test
  public void testExceptions() {
    assertThatThrownBy(
            () -> {
              engine.eval("var := undefinedVariable + 42;");
            })
        .isInstanceOf(UndefinedVariableException.class)
        .is(atPosition(0, 7, 24))
        .hasMessage("undefined variable undefinedVariable");

    assertThatThrownBy(
            () -> {
              engine.eval(
                  """
                    var := true and (10 +
                    10);\
                    """);
            })
        .isInstanceOf(FunctionNotFoundException.class)
        .is(atPosition(0, 1, 17, 2))
        .hasMessage("function 'and(Boolean, Long)' not found");
  }

  @Test
  public void testFunctions() throws ScriptException, NoSuchMethodException {
    VtlScriptEngine engine = (VtlScriptEngine) this.engine;
    engine.registerMethod("testTrim", Fun.toMethod(TextFunctions::testTrim));
    engine.registerMethod("testUpper", Fun.toMethod(TextFunctions::testUpper));

    engine.eval(
        """
                res := testUpper("  foo bar ");
                res := testTrim(res);\
                """);
    assertThat(engine.get("res")).isEqualTo("FOO BAR");
  }

  @Test
  public void testSyntaxError() {
    assertThatThrownBy(
            () -> {
              engine.eval("var := 40 + 42");
            })
        .isInstanceOf(VtlSyntaxException.class)
        .hasMessage("missing ';' at '<EOF>'")
        .is(atPosition(0, 14, 14));
  }

  //    @Test
  //    void testMatchParameters() throws NoSuchMethodException {
  //        var method = this.getClass().getMethod("test", Comparable.class, Comparable.class);
  //
  //        // Correct combination
  //        assertThat(VtlScriptEngine.matchParameters(method, String.class,
  // String.class)).isTrue();
  //        assertThat(VtlScriptEngine.matchParameters(method, Long.class, Long.class)).isTrue();
  //        assertThat(VtlScriptEngine.matchParameters(method, Double.class,
  // Double.class)).isTrue();
  //        assertThat(VtlScriptEngine.matchParameters(method, Boolean.class,
  // Boolean.class)).isTrue();
  //
  //        // Wrong types
  //        assertThat(VtlScriptEngine.matchParameters(method, Number.class,
  // Number.class)).isFalse();
  //        assertThat(VtlScriptEngine.matchParameters(method, VtlScriptEngine.class,
  // VtlScriptEngine.class)).isFalse();
  //
  //        // Wrong combination
  //        assertThat(VtlScriptEngine.matchParameters(method, Double.class, Long.class)).isFalse();
  //        assertThat(VtlScriptEngine.matchParameters(method, Long.class, Double.class)).isFalse();
  //        assertThat(VtlScriptEngine.matchParameters(method, Long.class, String.class)).isFalse();
  //        assertThat(VtlScriptEngine.matchParameters(method, Boolean.class,
  // String.class)).isFalse();
  //        assertThat(VtlScriptEngine.matchParameters(method, Long.class, String.class)).isFalse();
  //
  //        // Wrong number.
  //        assertThat(VtlScriptEngine.matchParameters(method, String.class)).isFalse();
  //        assertThat(VtlScriptEngine.matchParameters(method, String.class, String.class,
  // String.class)).isFalse();
  //    }

  @Test
  public void testSerialization() throws Exception {
    //        VtlScriptEngine engine = (VtlScriptEngine) this.engine;
    //        var o = new PipedOutputStream();
    //        var i = new PipedInputStream(o, 8192);
    //        var out = new ObjectOutputStream(o);
    //        var in = new ObjectInputStream(i);
    //
    //        var bar = ResolvableExpression.withType(String.class).using(c -> "bar");
    //        var baz = ResolvableExpression.withType(String.class).using(c -> "baz");
    //        var exprVisitor = new ExpressionVisitor(Map.of(), new InMemoryProcessingEngine(),
    // engine);
    //        var comparisonVisitor = new ComparisonVisitor(exprVisitor);
    //        var condition = comparisonVisitor.compareExpressions(bar, baz,
    // VtlScriptEngineTest::isEqual);
    //        var expr = ResolvableExpression.withTypeCasting(String.class, (clazz, ctx) ->
    //                condition.resolve(ctx) ?
    //                        clazz.cast(bar.resolve(ctx)) : clazz.cast(baz.resolve(ctx)));
    //        out.writeObject(expr);
    //
    //        var res = in.readObject();
    //        System.out.println(res);
  }

  @Test
  public void testRegisterGlobal() throws NoSuchMethodException, ScriptException {

    InMemoryDataset ds =
        new InMemoryDataset(
            List.of(
                new Structured.Component("name", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("age", Double.class, Dataset.Role.MEASURE),
                new Structured.Component("weight", Double.class, Dataset.Role.MEASURE)),
            Arrays.asList("Toto", null, 12.34),
            Arrays.asList("Hadrien", 12.34, 12.34),
            Arrays.asList("Nico", 12.34, 12.34));

    var doNothingMethod = DoNothingClass.class.getMethod("doNothingMethod");
    var ceilMethod = MathFunctions.class.getMethod("myCustomCeil", Double.class);

    VtlScriptEngine engine = (VtlScriptEngine) this.engine;
    engine.registerGlobalMethod("doNothingMethod", doNothingMethod);
    engine.registerMethod("myCustomCeil", ceilMethod);
    ScriptContext context = engine.getContext();
    context.setAttribute("ds", ds, ScriptContext.ENGINE_SCOPE);

    Object res1 = engine.eval("res := myCustomCeil(ds);");
    assertThat(res1).isNotNull();
  }

  public static class MathFunctions {
    public static Double myCustomCeil(Double op) {
      if (op == null) {
        return null;
      }
      return Math.ceil(op);
    }
  }

  public static class DoNothingClass {
    public static void doNothingMethod() {}
  }
}
