package fr.insee.vtl.engine.aggregation;

import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ResolvableExpression;
import java.util.Map;

/** Column references used inside aggregate expressions (e.g. {@code count()} in {@code having}). */
public final class AggregationColumnReferences {

  private AggregationColumnReferences() {}

  public static ResolvableExpression countMeasure(Positioned position) {
    return columnReference(position, AggregationNames.COUNT_MEASURE, Long.class);
  }

  public static ResolvableExpression columnReference(
      Positioned position, String columnName, Class<?> type) {
    return new ResolvableExpression(position) {
      @Override
      public Object resolve(Map<String, Object> context) {
        return context.get(columnName);
      }

      @Override
      public Class<?> getType() {
        return type;
      }
    };
  }
}
