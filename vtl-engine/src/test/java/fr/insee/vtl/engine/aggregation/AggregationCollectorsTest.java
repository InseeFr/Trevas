package fr.insee.vtl.engine.aggregation;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;
import static fr.insee.vtl.engine.utils.antlr.ParserTestVisitor.findFirstContextTyped;
import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.model.*;
import fr.insee.vtl.parser.VtlParser;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AggregationCollectorsTest {

  @Test
  void fromAggrDatasetInvocationBuildsCollectorsForAllMeasures() {
    VtlParser.AggrDatasetContext ctx =
        findFirstContextTyped("res := sum(ds1 group by id_1);", VtlParser.AggrDatasetContext.class);
    InMemoryDataset dataset =
        new InMemoryDataset(
            List.of(Map.of("id_1", 1L, "me_1", 2L, "me_2", 2.5D)),
            Map.of("id_1", Long.class, "me_1", Long.class, "me_2", Double.class),
            Map.of(
                "id_1",
                Dataset.Role.IDENTIFIER,
                "me_1",
                Dataset.Role.MEASURE,
                "me_2",
                Dataset.Role.MEASURE));

    DatasetExpression datasetExpression = DatasetExpression.of(dataset, fromContext(ctx));
    Map<String, AggregationExpression> collectors =
        AggregationCollectors.fromAggrDatasetInvocation(ctx, datasetExpression, fromContext(ctx));

    assertThat(collectors).containsOnlyKeys("me_1", "me_2");
    assertThat(collectors.get("me_1").getType()).isEqualTo(Long.class);
    assertThat(collectors.get("me_2").getType()).isEqualTo(Double.class);
  }

  @Test
  void fromAggrDatasetInvocationCountUsesIntVar() {
    VtlParser.AggrDatasetContext ctx =
        findFirstContextTyped(
            "res := count(ds1 group by id_1);", VtlParser.AggrDatasetContext.class);
    InMemoryDataset dataset =
        new InMemoryDataset(
            List.of(Map.of("id_1", 1L)),
            Map.of("id_1", Long.class),
            Map.of("id_1", Dataset.Role.IDENTIFIER));

    DatasetExpression datasetExpression = DatasetExpression.of(dataset, fromContext(ctx));
    Map<String, AggregationExpression> collectors =
        AggregationCollectors.fromAggrDatasetInvocation(ctx, datasetExpression, fromContext(ctx));

    assertThat(collectors).containsOnlyKeys(AggregationNames.COUNT_MEASURE);
    assertThat(collectors.get(AggregationNames.COUNT_MEASURE))
        .isInstanceOf(AggregationExpression.CountAggregationExpression.class);
  }

  @Test
  void fromAggrClauseMatchesEquivalentInvocationCollectors() {
    VtlParser.AggrDatasetContext invocationCtx =
        findFirstContextTyped("res := sum(ds1 group by id_1);", VtlParser.AggrDatasetContext.class);
    InMemoryDataset invocationDataset =
        new InMemoryDataset(
            List.of(Map.of("id_1", 1L, "me_1", 2L)),
            Map.of("id_1", Long.class, "me_1", Long.class),
            Map.of("id_1", Dataset.Role.IDENTIFIER, "me_1", Dataset.Role.MEASURE));
    DatasetExpression invocationExpression =
        DatasetExpression.of(invocationDataset, fromContext(invocationCtx));
    Map<String, AggregationExpression> invocationCollectors =
        AggregationCollectors.fromAggrDatasetInvocation(
            invocationCtx, invocationExpression, fromContext(invocationCtx));

    VtlParser.AggrClauseContext clauseCtx =
        findFirstContextTyped(
            "res := ds1[aggr me_1 := sum(me_1) group by id_1];", VtlParser.AggrClauseContext.class);
    Structured.DataStructure normalizedStructure =
        new InMemoryDataset(
                List.of(Map.of("id_1", 1L, "me_1", 2L)),
                Map.of("id_1", Long.class, "me_1", Long.class),
                Map.of("id_1", Dataset.Role.IDENTIFIER, "me_1", Dataset.Role.MEASURE))
            .getDataStructure();
    Map<String, AggregationExpression> clauseCollectors =
        AggregationCollectors.fromAggrClause(
            clauseCtx, normalizedStructure, fromContext(clauseCtx));

    assertThat(clauseCollectors).isEqualTo(invocationCollectors);
  }
}
