package fr.insee.vtl.engine.semantics.udo;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.antlr.runtime.CharStreams;
import fr.insee.vtl.antlr.runtime.CommonTokenStream;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlLexer;
import fr.insee.vtl.parser.VtlParser;
import org.junit.jupiter.api.Test;

class UdoRulesetTypeParserTest {

  private static final Positioned POS = () -> new Positioned.Position("test", 1, 1, 0, 0);

  @Test
  void parsesGenericRuleset() throws VtlScriptException {
    assertThat(UdoRulesetTypeParser.parse(parseRulesetType("ruleset"), POS))
        .isEqualTo(UdoRulesetKind.ANY);
  }

  @Test
  void parsesDatapointRuleset() throws VtlScriptException {
    assertThat(UdoRulesetTypeParser.parse(parseRulesetType("datapoint"), POS))
        .isEqualTo(UdoRulesetKind.DATAPOINT);
  }

  @Test
  void parsesHierarchicalRuleset() throws VtlScriptException {
    assertThat(UdoRulesetTypeParser.parse(parseRulesetType("hierarchical"), POS))
        .isEqualTo(UdoRulesetKind.HIERARCHICAL);
  }

  private static VtlParser.RulesetTypeContext parseRulesetType(String vtl) {
    VtlLexer lexer = new VtlLexer(CharStreams.fromString(vtl));
    VtlParser parser = new VtlParser(new CommonTokenStream(lexer));
    return parser.inputParameterType().rulesetType();
  }
}
