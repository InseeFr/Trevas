package fr.insee.vtl.engine.semantics.udo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import fr.insee.vtl.antlr.runtime.CharStreams;
import fr.insee.vtl.antlr.runtime.CommonTokenStream;
import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.VtlScriptEngineFactory;
import fr.insee.vtl.engine.exceptions.FunctionNotFoundException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.parser.VtlLexer;
import fr.insee.vtl.parser.VtlParser;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.script.ScriptContext;
import javax.script.ScriptEngineManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Call-site lookup uses the {@link ExpressionVisitor} map, not {@code ENGINE_SCOPE} alone — so a
 * UDO is resolved like a variable (nested calls, later closures).
 */
class UdoCallLookupTest {

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
  void callFindsUdoInVisitorContextEvenIfMissingFromEngineScope() {
    UdoDefinition udo =
        new UdoDefinition(
            "add",
            List.of(
                UdoParameter.mandatory("x", Long.class), UdoParameter.mandatory("y", Long.class)),
            Long.class,
            parseExpr("x + y"),
            engine);
    Map<String, Object> scope = new HashMap<>();
    scope.put("add", udo);

    var visitor = new ExpressionVisitor(scope, engine.getProcessingEngine(), engine);
    Object result = visitor.visit(parseExpr("add(1, 2)")).resolve(scope);

    assertThat(result).isEqualTo(3L);
    assertThat(engine.getBindings(ScriptContext.ENGINE_SCOPE).get("add")).isNull();
  }

  @Test
  void callDoesNotFallBackToEngineScope() throws Exception {
    engine.eval(
        """
        define operator add (x integer, y integer)
           returns integer is
              x + y
        end operator;
        """);
    assertThat(engine.getBindings(ScriptContext.ENGINE_SCOPE).get("add"))
        .isInstanceOf(UdoDefinition.class);

    var visitor = new ExpressionVisitor(Map.of(), engine.getProcessingEngine(), engine);
    assertThatThrownBy(() -> visitor.visit(parseExpr("add(1, 2)")))
        .hasRootCauseInstanceOf(FunctionNotFoundException.class);
  }

  private static VtlParser.ExprContext parseExpr(String vtl) {
    VtlLexer lexer = new VtlLexer(CharStreams.fromString(vtl));
    VtlParser parser = new VtlParser(new CommonTokenStream(lexer));
    return parser.expr();
  }
}
