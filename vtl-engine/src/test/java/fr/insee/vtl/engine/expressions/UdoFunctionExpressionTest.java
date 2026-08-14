package fr.insee.vtl.engine.expressions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import fr.insee.vtl.antlr.runtime.CharStreams;
import fr.insee.vtl.antlr.runtime.CommonTokenStream;
import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.VtlScriptEngineFactory;
import fr.insee.vtl.engine.semantics.udo.UdoDefinition;
import fr.insee.vtl.engine.semantics.udo.UdoParameter;
import fr.insee.vtl.model.ConstantExpression;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.parser.VtlLexer;
import fr.insee.vtl.parser.VtlParser;
import java.util.List;
import java.util.Map;
import javax.script.ScriptEngineManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests the UDO expression contract with a <strong>hardcoded</strong> {@link UdoDefinition} —
 * no {@code define operator} parse. Visitor wiring is replaced later by the real define path.
 */
class UdoFunctionExpressionTest {

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
  void hardcodedAddResolvesWithoutDefineOperator() {
    UdoDefinition udo =
        new UdoDefinition(
            "add",
            List.of(
                UdoParameter.mandatory("x", Long.class), UdoParameter.mandatory("y", Long.class)),
            Long.class,
            parseExpr("x + y"),
            engine);

    var expr =
        new UdoFunctionExpression(
            udo, List.of(new ConstantExpression(1L, POS), new ConstantExpression(2L, POS)), POS);

    assertThat(expr.getType()).isEqualTo(Long.class);
    assertThat(expr.resolve(Map.of())).isEqualTo(3L);
  }

  @Test
  void freeVarIsLookedUpInResolveContext() {
    UdoDefinition udo =
        new UdoDefinition(
            "max_with_y",
            List.of(UdoParameter.mandatory("x", Long.class)),
            Long.class,
            parseExpr("if x > y then x else y"),
            engine);

    var expr = new UdoFunctionExpression(udo, List.of(new ConstantExpression(2L, POS)), POS);

    assertThat(expr.resolve(Map.of("y", 4L))).isEqualTo(4L);
  }

  @Test
  void declaredReturnMismatchIsRejected() {
    UdoDefinition udo =
        new UdoDefinition(
            "max1",
            List.of(
                UdoParameter.mandatory("x", Long.class), UdoParameter.mandatory("y", Long.class)),
            Boolean.class,
            parseExpr("if x > y then x else y"),
            engine);

    var expr =
        new UdoFunctionExpression(
            udo, List.of(new ConstantExpression(3L, POS), new ConstantExpression(7L, POS)), POS);

    assertThatThrownBy(() -> expr.resolve(Map.of())).hasMessageContaining("boolean");
  }

  private static VtlParser.ExprContext parseExpr(String vtl) {
    VtlLexer lexer = new VtlLexer(CharStreams.fromString(vtl));
    VtlParser parser = new VtlParser(new CommonTokenStream(lexer));
    return parser.expr();
  }
}
