package fr.insee.vtl.engine.set;

import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.Structured;
import java.util.List;

/** VTL set-operator orchestration ({@code union}, etc.). */
public final class UnionOperations {

  private UnionOperations() {}

  public static DatasetExpression union(ProcessingEngine engine, List<DatasetExpression> datasets) {
    List<String> dedupeOn =
        datasets.get(0).getIdentifiers().stream().map(Structured.Component::getName).toList();
    return engine.executeUnion(datasets, dedupeOn);
  }
}
