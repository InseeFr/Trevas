package fr.insee.vtl.engine.semantics.udo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import fr.insee.vtl.antlr.runtime.CharStreams;
import fr.insee.vtl.antlr.runtime.CommonTokenStream;
import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.VtlScriptEngineFactory;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlLexer;
import fr.insee.vtl.parser.VtlParser;
import java.util.List;
import java.util.Map;
import javax.script.ScriptEngineManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for call-site arg wiring (defaults, {@code _}, arity) before {@code resolve}. */
class UdoInvokeExecutorTest {

  private static final Positioned POS = () -> new Positioned.Position("test", 1, 1, 0, 0);

  private VtlScriptEngine engine;

  @BeforeEach
  void setUp() {
    var fromManager = new ScriptEngineManager().getEngineByName("vtl");
    if (fromManager instanceof VtlScriptEngine vtl) {
      engine = vtl;
    } else {
      engine = (VtlScriptEngine) new VtlScriptEngineFactory().getScriptEngine();
    }
    assertThat(engine).isNotNull();
  }

  @Test
  void missingTrailingArgsUseDefaults() throws VtlScriptException {
    UdoDefinition udo = addWithDefaults();
    var visitor = new ExpressionVisitor(Map.of(), engine.getProcessingEngine(), engine);

    ResolvableExpression oneArg =
        UdoInvokeExecutor.invoke(udo, parseCall("add(5)"), visitor, engine, POS);
    ResolvableExpression noArgs =
        UdoInvokeExecutor.invoke(udo, parseCall("add()"), visitor, engine, POS);

    assertThat(oneArg.resolve(Map.of())).isEqualTo(5L);
    assertThat(noArgs.resolve(Map.of())).isEqualTo(0L);
  }

  @Test
  void optionalUnderscoreUsesDefault() throws VtlScriptException {
    UdoDefinition udo = addWithDefaults();
    var visitor = new ExpressionVisitor(Map.of(), engine.getProcessingEngine(), engine);

    ResolvableExpression expr =
        UdoInvokeExecutor.invoke(udo, parseCall("add(10, _)"), visitor, engine, POS);

    assertThat(expr.resolve(Map.of())).isEqualTo(10L);
  }

  @Test
  void missingMandatoryArgIsRejected() {
    UdoDefinition udo =
        new UdoDefinition(
            "add2",
            List.of(
                UdoParameter.mandatory("x", Long.class), UdoParameter.mandatory("y", Long.class)),
            Long.class,
            parseExpr("x + y"),
            engine);
    var visitor = new ExpressionVisitor(Map.of(), engine.getProcessingEngine(), engine);

    assertThatThrownBy(
            () -> UdoInvokeExecutor.invoke(udo, parseCall("add2(1)"), visitor, engine, POS))
        .isInstanceOf(VtlScriptException.class)
        .hasMessageContaining("missing mandatory");
  }

  @Test
  void tooManyArgsIsRejected() {
    UdoDefinition udo = addWithDefaults();
    var visitor = new ExpressionVisitor(Map.of(), engine.getProcessingEngine(), engine);

    assertThatThrownBy(
            () -> UdoInvokeExecutor.invoke(udo, parseCall("add(1, 2, 3)"), visitor, engine, POS))
        .isInstanceOf(VtlScriptException.class)
        .hasMessageContaining("too many arguments");
  }

  @Test
  void optionalWithoutDefaultIsRejected() {
    UdoDefinition udo =
        new UdoDefinition(
            "add2",
            List.of(
                UdoParameter.mandatory("x", Long.class), UdoParameter.mandatory("y", Long.class)),
            Long.class,
            parseExpr("x + y"),
            engine);
    var visitor = new ExpressionVisitor(Map.of(), engine.getProcessingEngine(), engine);

    assertThatThrownBy(
            () -> UdoInvokeExecutor.invoke(udo, parseCall("add2(1, _)"), visitor, engine, POS))
        .isInstanceOf(VtlScriptException.class)
        .hasMessageContaining("OPTIONAL");
  }

  private UdoDefinition addWithDefaults() {
    return new UdoDefinition(
        "add",
        List.of(
            UdoParameter.withDefault("x", Long.class, 0L),
            UdoParameter.withDefault("y", Long.class, 0L)),
        Long.class,
        parseExpr("x + y"),
        engine);
  }

  private static VtlParser.CallDatasetContext parseCall(String vtl) {
    VtlParser.ExprContext expr = parseExpr(vtl);
    if (!(expr instanceof VtlParser.FunctionsExpressionContext functionsExpr)) {
      throw new IllegalArgumentException("expected a function call expression: " + vtl);
    }
    VtlParser.FunctionsContext functions = functionsExpr.functions();
    if (!(functions instanceof VtlParser.GenericFunctionsContext genericFunctions)) {
      throw new IllegalArgumentException("expected generic function call: " + vtl);
    }
    VtlParser.GenericOperatorsContext operators = genericFunctions.genericOperators();
    if (!(operators instanceof VtlParser.CallDatasetContext call)) {
      throw new IllegalArgumentException("expected callDataset: " + vtl);
    }
    return call;
  }

  private static VtlParser.ExprContext parseExpr(String vtl) {
    VtlLexer lexer = new VtlLexer(CharStreams.fromString(vtl));
    VtlParser parser = new VtlParser(new CommonTokenStream(lexer));
    return parser.expr();
  }
}
