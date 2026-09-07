package fr.insee.vtl.engine.visitors;

import static fr.insee.vtl.engine.utils.antlr.ParserTestVisitor.findFirstContextTyped;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.parser.VtlParser;
import java.util.List;
import org.junit.jupiter.api.Test;

class GroupByVisitorTest {

  private static final Structured.DataStructure STRUCTURE =
      new Structured.DataStructure(
          List.of(
              new Structured.Component("Id_1", Long.class, Dataset.Role.IDENTIFIER),
              new Structured.Component("Id_2", String.class, Dataset.Role.IDENTIFIER),
              new Structured.Component("Id_3", String.class, Dataset.Role.IDENTIFIER),
              new Structured.Component("Me_1", Long.class, Dataset.Role.MEASURE)));

  @Test
  void groupByReturnsListedComponents() {
    VtlParser.GroupingClauseContext grouping =
        findFirstContextTyped(
                "res := ds[aggr m := min(Me_1) group by Id_1, Id_2];",
                VtlParser.AggrClauseContext.class)
            .groupingClause();

    assertThat(new GroupByVisitor(STRUCTURE).visit(grouping)).containsExactly("Id_1", "Id_2");
  }

  @Test
  void groupExceptReturnsComplementOfIdentifiersInStructureOrder() {
    VtlParser.GroupingClauseContext grouping =
        findFirstContextTyped(
                "res := ds[aggr m := min(Me_1) group except Id_3];",
                VtlParser.AggrClauseContext.class)
            .groupingClause();

    List<String> keys = new GroupByVisitor(STRUCTURE).visit(grouping);

    // Regression: old bug returned the except list itself (Id_3) instead of Id_1, Id_2.
    assertThat(keys).containsExactly("Id_1", "Id_2").doesNotContain("Id_3", "Me_1");
  }

  @Test
  void groupExceptRejectsNonIdentifier() {
    VtlParser.GroupingClauseContext grouping =
        findFirstContextTyped(
                "res := ds[aggr m := min(Me_1) group except Me_1];",
                VtlParser.AggrClauseContext.class)
            .groupingClause();

    assertThatThrownBy(() -> new GroupByVisitor(STRUCTURE).visit(grouping))
        .isInstanceOf(VtlRuntimeException.class)
        .hasMessageContaining("not an identifier");
  }

  @Test
  void groupExceptRejectsUnknownComponent() {
    VtlParser.GroupingClauseContext grouping =
        findFirstContextTyped(
                "res := ds[aggr m := min(Me_1) group except Missing];",
                VtlParser.AggrClauseContext.class)
            .groupingClause();

    assertThatThrownBy(() -> new GroupByVisitor(STRUCTURE).visit(grouping))
        .isInstanceOf(VtlRuntimeException.class)
        .hasMessageContaining("unknown component");
  }
}
