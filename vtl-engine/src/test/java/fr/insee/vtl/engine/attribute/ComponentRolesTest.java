package fr.insee.vtl.engine.attribute;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.antlr.runtime.CharStreams;
import fr.insee.vtl.antlr.runtime.CommonTokenStream;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.parser.VtlLexer;
import fr.insee.vtl.parser.VtlParser;
import org.junit.jupiter.api.Test;

class ComponentRolesTest {

  @Test
  void resolvesViralAttributeRoleFromCalcClause() {
    VtlParser.CalcClauseContext calc =
        parseCalc("DS := ds1[calc viral attribute At_1 := \"x\"];\n");
    assertThat(ComponentRoles.fromParser(calc.calcClauseItem(0).componentRole()))
        .isEqualTo(Dataset.Role.VIRALATTRIBUTE);
  }

  @Test
  void resolvesAttributeAndMeasureRoles() {
    VtlParser.CalcClauseContext calc =
        parseCalc("DS := ds1[calc attribute At_1 := \"x\", measure Me_2 := 1];\n");
    assertThat(ComponentRoles.fromParser(calc.calcClauseItem(0).componentRole()))
        .isEqualTo(Dataset.Role.ATTRIBUTE);
    assertThat(ComponentRoles.fromParser(calc.calcClauseItem(1).componentRole()))
        .isEqualTo(Dataset.Role.MEASURE);
  }

  private static VtlParser.CalcClauseContext parseCalc(String script) {
    VtlLexer lexer = new VtlLexer(CharStreams.fromString(script));
    VtlParser parser = new VtlParser(new CommonTokenStream(lexer));
    VtlParser.StatementContext stmt = parser.start().statement(0);
    if (!(stmt instanceof VtlParser.TemporaryAssignmentContext assignment)) {
      throw new IllegalStateException("expected temporary assignment in: " + script);
    }
    if (!(assignment.expr() instanceof VtlParser.ClauseExprContext clauseExpr)) {
      throw new IllegalStateException("expected clause expression in: " + script);
    }
    VtlParser.CalcClauseContext calc = clauseExpr.datasetClause().calcClause();
    if (calc == null) {
      throw new IllegalStateException("expected calc clause in: " + script);
    }
    return calc;
  }
}
