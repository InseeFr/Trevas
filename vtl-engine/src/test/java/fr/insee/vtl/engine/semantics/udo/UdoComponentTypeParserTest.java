package fr.insee.vtl.engine.semantics.udo;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.antlr.runtime.CharStreams;
import fr.insee.vtl.antlr.runtime.CommonTokenStream;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlLexer;
import fr.insee.vtl.parser.VtlParser;
import org.junit.jupiter.api.Test;

class UdoComponentTypeParserTest {

  private static final Positioned POS = () -> new Positioned.Position("test", 1, 1, 0, 0);

  @Test
  void parsesMeasureWithInteger() throws VtlScriptException {
    var signature = UdoComponentTypeParser.parse(parseComponentType("measure < integer >"), POS);

    assertThat(signature.role()).isEqualTo(Dataset.Role.MEASURE);
    assertThat(signature.scalarType()).isEqualTo(Long.class);
  }

  @Test
  void parsesUnconstrainedAttribute() throws VtlScriptException {
    var signature = UdoComponentTypeParser.parse(parseComponentType("attribute"), POS);

    assertThat(signature.role()).isEqualTo(Dataset.Role.ATTRIBUTE);
    assertThat(signature.scalarType()).isNull();
  }

  @Test
  void parsesViralAttributeWithString() throws VtlScriptException {
    var signature =
        UdoComponentTypeParser.parse(parseComponentType("viral attribute < string >"), POS);

    assertThat(signature.role()).isEqualTo(Dataset.Role.VIRALATTRIBUTE);
    assertThat(signature.scalarType()).isEqualTo(String.class);
  }

  private static VtlParser.ComponentTypeContext parseComponentType(String vtl) {
    VtlLexer lexer = new VtlLexer(CharStreams.fromString(vtl));
    VtlParser parser = new VtlParser(new CommonTokenStream(lexer));
    return parser.inputParameterType().componentType();
  }
}
