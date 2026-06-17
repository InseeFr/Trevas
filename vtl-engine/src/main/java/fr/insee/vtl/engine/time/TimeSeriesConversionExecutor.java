package fr.insee.vtl.engine.time;

import fr.insee.vtl.engine.expressions.ComponentExpression;
import fr.insee.vtl.engine.visitors.expression.functions.GenericFunctionsVisitor;
import fr.insee.vtl.model.*;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** VTL time-series conversions ({@code flow_to_stock}, {@code stock_to_flow}). */
public final class TimeSeriesConversionExecutor {

  private TimeSeriesConversionExecutor() {}

  public static DatasetExpression stockToFlow(
      ProcessingEngine engine,
      GenericFunctionsVisitor genericFunctions,
      DatasetExpression dataset,
      Structured.Component timeColumn,
      Positioned position)
      throws VtlScriptException {
    var orderBy =
        dataset.getIdentifiers().stream()
            .collect(
                Collectors.toMap(
                    Structured.Component::getName,
                    c -> Analytics.Order.ASC,
                    (a, b) -> b,
                    LinkedHashMap::new));
    var partition =
        orderBy.keySet().stream()
            .filter(name -> !timeColumn.getName().equals(name))
            .collect(Collectors.toList());

    for (Structured.Component measure : dataset.getMeasures()) {
      if (!Number.class.isAssignableFrom(measure.getType())) {
        continue;
      }
      String measureName = measure.getName();
      String lagColumnName = measureName + "_lag";

      DatasetExpression lag =
          engine.executeLeadOrLagAn(
              dataset, measureName, Analytics.Function.LAG, measureName, 1, partition, orderBy);
      lag = engine.executeRename(lag, Map.of(measureName, lagColumnName));
      lag =
          engine.executeProject(
              lag,
              Stream.concat(
                      dataset.getIdentifiers().stream().map(Structured.Component::getName),
                      Stream.of(lagColumnName))
                  .collect(Collectors.toList()));

      dataset =
          engine.executeLeftJoin(Map.of("left", dataset, "lag", lag), dataset.getIdentifiers());

      var measureExpr =
          new ComponentExpression(dataset.getDataStructure().get(measureName), position);
      var lagExpr =
          new ComponentExpression(dataset.getDataStructure().get(lagColumnName), position);
      var nvlExpr =
          genericFunctions.invokeFunction(
              "nvl", List.of(lagExpr, new ConstantExpression(0L, position)), position);
      var subtractionExpr =
          genericFunctions.invokeFunction("subtraction", List.of(measureExpr, nvlExpr), position);

      dataset =
          engine.executeCalc(
              dataset,
              Map.of(measureName, subtractionExpr),
              Map.of(measureName, Dataset.Role.MEASURE),
              Map.of());
      dataset =
          engine.executeProject(
              dataset,
              dataset.getColumnNames().stream()
                  .filter(name -> !name.equals(lagColumnName))
                  .collect(Collectors.toList()));
    }
    return dataset;
  }

  public static DatasetExpression flowToStock(
      ProcessingEngine engine, DatasetExpression dataset, Structured.Component timeColumn) {
    var orderBy =
        dataset.getIdentifiers().stream()
            .collect(
                Collectors.toMap(
                    Structured.Component::getName,
                    c -> Analytics.Order.ASC,
                    (a, b) -> b,
                    LinkedHashMap::new));
    var partition =
        orderBy.keySet().stream()
            .filter(name -> !timeColumn.getName().equals(name))
            .collect(Collectors.toList());

    for (Structured.Component measure : dataset.getMeasures()) {
      dataset =
          engine.executeSimpleAnalytic(
              dataset,
              measure.getName(),
              Analytics.Function.SUM,
              measure.getName(),
              partition,
              orderBy,
              null);
    }
    return dataset;
  }
}
