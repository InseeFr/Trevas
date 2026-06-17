package fr.insee.vtl.engine.validation;

import fr.insee.vtl.model.DatasetExpression;
import java.util.Map;

/** Spark-specific capabilities required by {@code check_hierarchy} (not general VTL transforms). */
public interface HierarchicalValidationRuntime {

  Map<String, Object> columnBindings(
      DatasetExpression dataset, String keyColumn, String valueColumn);

  /** Filter; keeps one row when the slice would be empty (value-domain value absent). */
  DatasetExpression filterKeepingSchema(DatasetExpression dataset, String filterText);
}
