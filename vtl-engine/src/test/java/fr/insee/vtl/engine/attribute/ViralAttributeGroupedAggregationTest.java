package fr.insee.vtl.engine.attribute;

import fr.insee.vtl.engine.processors.InMemoryProcessingEngine;
import fr.insee.vtl.model.AggregationExpression;
import fr.insee.vtl.model.AggregationViralPropagation;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ResolvableExpression;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Grouped {@code aggr} propagates viral {@code At_1} values per {@code Id_1}. */
class ViralAttributeGroupedAggregationTest {

  private static final Positioned TEST_POSITION = () -> new Positioned.Position("test", 1, 1, 0, 0);

  @Test
  void groupedAggrPropagatesViralAttributeValues() {
    InMemoryDataset input = GroupedAggrViralFixtures.dataset();

    Map<String, AggregationExpression> collectors = new LinkedHashMap<>();
    collectors.put(
        "Me_2",
        AggregationExpression.max(
            ResolvableExpression.withType(Long.class)
                .withPosition(TEST_POSITION)
                .using(dp -> (Long) dp.get("Me_1"))));
    collectors.put(
        "Me_3",
        AggregationExpression.min(
            ResolvableExpression.withType(Long.class)
                .withPosition(TEST_POSITION)
                .using(dp -> (Long) dp.get("Me_1"))));

    DatasetExpression inputExpression = DatasetExpression.of(input, TEST_POSITION);
    Dataset result =
        new InMemoryProcessingEngine()
            .executeAggr(
                inputExpression,
                List.of("Id_1"),
                collectors,
                AggregationViralPropagation.AGGR_CLAUSE_GROUPED)
            .resolve(Map.of());

    GroupedAggrViralFixtures.assertGroupedAggrViralValues(result.getDataAsMap());
  }
}
