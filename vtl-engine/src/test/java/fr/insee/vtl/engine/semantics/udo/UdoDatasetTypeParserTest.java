package fr.insee.vtl.engine.semantics.udo;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.antlr.runtime.CharStreams;
import fr.insee.vtl.antlr.runtime.CommonTokenStream;
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
    UdoDatasetSignature signature =
        UdoDatasetTypeParser.parse(
            parseDatasetType(
                "dataset { identifier < string > id, measure < integer > long1 }"),
            POS);

    assertThat(signature.namedComponents())
        .containsExactly(
            new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER),
            new Structured.Component("long1", Long.class, Dataset.Role.MEASURE));
    assertThat(signature.wildcards()).isEmpty();
  }

  @Test
  void parsesWildcardMeasure() throws VtlScriptException {
    UdoDatasetSignature signature =
        UdoDatasetTypeParser.parse(
            parseDatasetType(
                "dataset { identifier < string > id, measure < integer > _ }"),
            POS);

    assertThat(signature.namedComponents())
        .containsExactly(new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER));
    assertThat(signature.wildcards())
        .containsExactly(
            new UdoDatasetSignature.Wildcard(
                Dataset.Role.MEASURE,
                Long.class,
                UdoDatasetSignature.WildcardMultiplicity.EXACTLY_ONE));
  }

  @Test
  void parsesZeroOrMoreAttributes() throws VtlScriptException {
    UdoDatasetSignature signature =
        UdoDatasetTypeParser.parse(
            parseDatasetType("dataset { measure < boolean > _, attribute < string > _* }"), POS);

    assertThat(signature.wildcards())
        .containsExactly(
            new UdoDatasetSignature.Wildcard(
                Dataset.Role.MEASURE,
                Boolean.class,
                UdoDatasetSignature.WildcardMultiplicity.EXACTLY_ONE),
            new UdoDatasetSignature.Wildcard(
                Dataset.Role.ATTRIBUTE,
                String.class,
                UdoDatasetSignature.WildcardMultiplicity.ZERO_OR_MORE));
  }

  private static VtlParser.DatasetTypeContext parseDatasetType(String vtl) {
    VtlLexer lexer = new VtlLexer(CharStreams.fromString(vtl));
    VtlParser parser = new VtlParser(new CommonTokenStream(lexer));
    return parser.inputParameterType().datasetType();
  }
}
