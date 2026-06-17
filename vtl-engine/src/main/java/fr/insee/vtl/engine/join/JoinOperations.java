package fr.insee.vtl.engine.join;

import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.Structured.Component;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** VTL join orchestration on top of mechanical {@link ProcessingEngine} join primitives. */
public final class JoinOperations {

  private JoinOperations() {}

  /**
   * Identifier components referenced across operands (deduplicated by {@link Component#equals}),
   * matching legacy {@code executeInnerJoin(Map)} key inference.
   */
  public static List<Component> inferredJoinKeys(Map<String, DatasetExpression> datasets) {
    return datasets.values().stream()
        .flatMap(dataset -> dataset.getDataStructure().values().stream())
        .filter(Component::isIdentifier)
        .collect(
            Collectors.collectingAndThen(
                Collectors.toCollection(LinkedHashSet::new), ArrayList::new));
  }

  public static DatasetExpression innerJoinInferringKeys(
      ProcessingEngine engine, Map<String, DatasetExpression> datasets) {
    return engine.executeInnerJoin(datasets, inferredJoinKeys(datasets));
  }
}
