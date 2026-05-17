package fr.insee.vtl.spark.aggregation;

import static org.apache.spark.sql.functions.min;

import fr.insee.vtl.engine.attribute.ViralAttributeCollectors;
import fr.insee.vtl.model.AggregationExpression;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.spark.SparkUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Column;

/** Spark columns for viral attribute propagation during {@code groupBy.agg}. */
public final class SparkViralAttributeAggregations {

  private SparkViralAttributeAggregations() {}

  public static List<Column> allAggregationColumns(
      Structured.DataStructure input,
      Structured.DataStructure output,
      Map<String, AggregationExpression> measureCollectors,
      java.util.function.BiFunction<String, AggregationExpression, Column> measureConverter) {
    Map<String, AggregationExpression> merged =
        ViralAttributeCollectors.mergeMeasureCollectors(input, output, measureCollectors);
    List<Column> columns = new ArrayList<>();
    for (Map.Entry<String, AggregationExpression> entry : merged.entrySet()) {
      String name = entry.getKey();
      if (measureCollectors.containsKey(name)) {
        columns.add(measureConverter.apply(name, entry.getValue()));
      } else {
        columns.add(min(SparkUtils.safeCol(name)).alias(name));
      }
    }
    return columns;
  }
}
