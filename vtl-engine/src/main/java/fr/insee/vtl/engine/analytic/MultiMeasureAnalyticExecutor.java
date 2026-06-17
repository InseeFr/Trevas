package fr.insee.vtl.engine.analytic;

import fr.insee.vtl.engine.join.JoinOperations;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.Structured;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Applies a window/analytic function per measure, then inner-joins the mono-measure results (VTL
 * dataset-level analytic invocation).
 */
public final class MultiMeasureAnalyticExecutor {

  @FunctionalInterface
  public interface PerMeasureAnalytic {
    DatasetExpression apply(DatasetExpression monoMeasureDataset, String targetColumnName);
  }

  private MultiMeasureAnalyticExecutor() {}

  public static DatasetExpression execute(
      ProcessingEngine engine,
      DatasetExpression dataset,
      String operandDatasetName,
      String analyticPrefix,
      PerMeasureAnalytic analytic) {
    List<Structured.Component> identifiers = dataset.getDataStructure().getIdentifiers();
    Map<String, DatasetExpression> perMeasure = new LinkedHashMap<>();

    for (Structured.Component measure : dataset.getDataStructure().getMeasures()) {
      List<String> columnNames =
          Stream.concat(identifiers.stream(), Stream.of(measure))
              .map(Structured.Component::getName)
              .collect(Collectors.toList());

      DatasetExpression mono =
          engine.executeRename(
              engine.executeProject(dataset, columnNames),
              Map.of(measure.getName(), operandDatasetName));

      String targetColumnName = analyticPrefix + "_" + measure.getName();
      DatasetExpression result = analytic.apply(mono, targetColumnName);

      result = engine.executeRename(result, Map.of(targetColumnName, measure.getName()));
      result =
          engine.executeProject(
              result,
              result.getColumnNames().stream()
                  .filter(name -> !name.equals(operandDatasetName))
                  .collect(Collectors.toList()));

      perMeasure.put(targetColumnName, result);
    }

    return JoinOperations.innerJoinInferringKeys(engine, perMeasure);
  }
}
