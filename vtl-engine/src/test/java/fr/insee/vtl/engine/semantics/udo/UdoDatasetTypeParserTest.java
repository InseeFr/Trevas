package fr.insee.vtl.engine.semantics.udo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import fr.insee.vtl.antlr.runtime.CharStreams;
import fr.insee.vtl.antlr.runtime.CommonTokenStream;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlLexer;
import fr.insee.vtl.parser.VtlParser;
import org.junit.jupiter.api.Test;

class UdoDatasetTypeParserTest {

  private static final Positioned POS = () -> new Positioned.Position("test", 1, 1, 0, 0);

  @Test
  void opaqueDatasetReturnsNull() throws VtlScriptException {
    assertThat(UdoDatasetTypeParser.parse(parseDatasetType("dataset"), POS)).isNull();
  }

  @Test
  void parsesDs4Signature() throws VtlScriptException {
    Structured.DataStructure structure =
        UdoDatasetTypeParser.parse(
            parseDatasetType(
                "dataset { identifier < string > id, measure < integer > long1 }"),
            POS);

    assertThat(structure.componentsInOrder())
        .containsExactly(
            new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER),
            new Structured.Component("long1", Long.class, Dataset.Role.MEASURE));
  }

  @Test
  void wildcardComponentIsRejected() {
    assertThatThrownBy(
            () ->
                UdoDatasetTypeParser.parse(
                    parseDatasetType("dataset { measure < integer > _ }"), POS))
        .isInstanceOf(VtlRuntimeException.class)
        .hasMessageContaining("wildcards");
  }

  private static VtlParser.DatasetTypeContext parseDatasetType(String vtl) {
    VtlLexer lexer = new VtlLexer(CharStreams.fromString(vtl));
    VtlParser parser = new VtlParser(new CommonTokenStream(lexer));
    return parser.inputParameterType().datasetType();
  }
}
