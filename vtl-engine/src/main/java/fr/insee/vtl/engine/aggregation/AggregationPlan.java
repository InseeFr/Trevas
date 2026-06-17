package fr.insee.vtl.engine.aggregation;

import fr.insee.vtl.engine.attribute.ViralAttributeCollectors;
import fr.insee.vtl.model.AggregationExpression;
import fr.insee.vtl.model.AggregationViralPropagation;
import fr.insee.vtl.model.Structured.DataStructure;
import java.util.List;
import java.util.Map;

/** Output structure and collectors for {@link fr.insee.vtl.model.ProcessingEngine#executeAggr}. */
public final class AggregationPlan {

  private AggregationPlan() {}

  public static Prepared prepare(
      DataStructure input,
      List<String> groupBy,
      Map<String, AggregationExpression> measureCollectors,
      AggregationViralPropagation viralPropagation) {
    DataStructure structure =
        AggregationResultStructureBuilder.build(
            input, groupBy, measureCollectors, viralPropagation);
    Map<String, AggregationExpression> collectors =
        ViralAttributeCollectors.mergeMeasureCollectors(
            input, structure, measureCollectors, viralPropagation);
    return new Prepared(structure, collectors);
  }

  public record Prepared(DataStructure structure, Map<String, AggregationExpression> collectors) {}
}
