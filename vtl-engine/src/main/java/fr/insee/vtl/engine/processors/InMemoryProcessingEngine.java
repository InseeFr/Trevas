package fr.insee.vtl.engine.processors;

import static fr.insee.vtl.model.Structured.*;

import fr.insee.vtl.engine.utils.KeyExtractor;
import fr.insee.vtl.engine.utils.MapCollector;
import fr.insee.vtl.model.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.script.ScriptEngine;

/**
 * The <code>InMemoryProcessingEngine</code> class is an implementation of a VTL engine that
 * performs all operations in memory.
 */
public class InMemoryProcessingEngine implements ProcessingEngine {

  @Override
  public DatasetExpression executeCalc(
      DatasetExpression expression,
      Map<String, ResolvableExpression> expressions,
      Map<String, Dataset.Role> roles,
      Map<String, String> expressionStrings) {

    // Copy the structure and mutate based on the expressions.
    var newStructure = new DataStructure(expression.getDataStructure());
    for (String columnName : expressions.keySet()) {
      // TODO: refine nullable strategy
      newStructure.put(
          columnName,
          new Dataset.Component(
              columnName, expressions.get(columnName).getType(), roles.get(columnName), true));
    }

    return new DatasetExpression(expression) {
      @Override
      public Dataset resolve(Map<String, Object> context) {
        var dataset = expression.resolve(context);
        List<List<Object>> result =
            dataset.getDataPoints().stream()
                .map(
                    dataPoint -> {
                      var newDataPoint = new DataPoint(newStructure, dataPoint);
                      for (String columnName : expressions.keySet()) {
                        newDataPoint.set(
                            columnName, expressions.get(columnName).resolve(dataPoint));
                      }
                      return newDataPoint;
                    })
                .collect(Collectors.toList());
        return new InMemoryDataset(result, newStructure);
      }

      @Override
      public DataStructure getDataStructure() {
        return newStructure;
      }
    };
  }

  @Override
  public DatasetExpression executeFilter(
      DatasetExpression expression, ResolvableExpression filter, String filterText) {
    return new DatasetExpression(expression) {

      @Override
      public DataStructure getDataStructure() {
        return expression.getDataStructure();
      }

      @Override
      public Dataset resolve(Map<String, Object> context) {
        Dataset resolve = expression.resolve(context);
        List<List<Object>> result =
            resolve.getDataPoints().stream()
                .filter(
                    map -> {
                      var res = filter.resolve(map);
                      if (res == null) return false;
                      return (boolean) res;
                    })
                .collect(Collectors.toList());
        return new InMemoryDataset(result, getDataStructure());
      }
    };
  }

  @Override
  public DatasetExpression executeRename(DatasetExpression expression, Map<String, String> fromTo) {
    if (fromTo.isEmpty()) {
      return expression;
    }
    var structure =
        expression.getDataStructure().values().stream()
            .map(
                component ->
                    !fromTo.containsKey(component.getName())
                        ? component
                        : new Dataset.Component(
                            fromTo.get(component.getName()),
                            component.getType(),
                            component.getRole(),
                            component.getNullable()))
            .collect(Collectors.toList());
    DataStructure renamedStructure = new DataStructure(structure);
    return new DatasetExpression(expression) {
      @Override
      public Dataset resolve(Map<String, Object> context) {
        var result =
            expression.resolve(context).getDataPoints().stream()
                .map(
                    dataPoint -> {
                      var newDataPoint = new DataPoint(renamedStructure, dataPoint);
                      for (String fromName : fromTo.keySet()) {
                        var toName = fromTo.get(fromName);
                        newDataPoint.set(toName, dataPoint.get(fromName));
                      }
                      return newDataPoint;
                    })
                .collect(Collectors.toList());
        return new InMemoryDataset(result, getDataStructure());
      }

      @Override
      public DataStructure getDataStructure() {
        return renamedStructure;
      }
    };
  }

  @Override
  public DatasetExpression executeProject(DatasetExpression expression, List<String> columnNames) {

    var structure =
        expression.getDataStructure().values().stream()
            .filter(component -> columnNames.contains(component.getName()))
            .collect(Collectors.toList());
    var newStructure = new DataStructure(structure);

    return new DatasetExpression(expression) {
      @Override
      public Dataset resolve(Map<String, Object> context) {
        var columnNames = getColumnNames();
        List<List<Object>> result =
            expression.resolve(context).getDataPoints().stream()
                .map(
                    data -> {
                      var projectedDataPoint = new DataPoint(newStructure);
                      for (String column : columnNames) {
                        projectedDataPoint.set(column, data.get(column));
                      }
                      return projectedDataPoint;
                    })
                .collect(Collectors.toList());
        // TODO: Use List<Datapoint> type for result to avoid conversion.
        return new InMemoryDataset(result, getDataStructure());
      }

      @Override
      public DataStructure getDataStructure() {
        return newStructure;
      }
    };
  }

  @Override
  public DatasetExpression executeUnion(List<DatasetExpression> datasets) {
    return new DatasetExpression(datasets.get(0)) {
      @Override
      public Dataset resolve(Map<String, Object> context) {
        Stream<DataPoint> stream = Stream.empty();
        for (DatasetExpression datasetExpression : datasets) {
          var dataset = datasetExpression.resolve(context);
          stream = Stream.concat(stream, dataset.getDataPoints().stream());
        }
        List<DataPoint> data = stream.distinct().collect(Collectors.toList());
        return new InMemoryDataset(data, getDataStructure());
      }

      @Override
      public DataStructure getDataStructure() {
        return (datasets.get(0)).getDataStructure();
      }
    };
  }

  @Override
  public DatasetExpression executeAggr(
      DatasetExpression expression,
      List<String> groupBy,
      Map<String, AggregationExpression> collectorMap) {
    // Create a keyExtractor with the columns we group by.
    var keyExtractor = new KeyExtractor(groupBy);

    // Compute the new data structure.
    Map<String, Dataset.Component> newStructure = new LinkedHashMap<>();
    for (Dataset.Component component : expression.getDataStructure().values()) {
      if (groupBy.contains(component.getName())) {
        newStructure.put(component.getName(), component);
      }
    }
    for (Map.Entry<String, AggregationExpression> entry : collectorMap.entrySet()) {
      // TODO: refine nullable strategy
      newStructure.put(
          entry.getKey(),
          new Dataset.Component(
              entry.getKey(), entry.getValue().getType(), Dataset.Role.MEASURE, true));
    }

    Structured.DataStructure structure = new Structured.DataStructure(newStructure.values());
    return new DatasetExpression(expression) {
      @Override
      public Dataset resolve(Map<String, Object> context) {

        List<DataPoint> data = expression.resolve(Map.of()).getDataPoints();
        MapCollector collector = new MapCollector(structure, collectorMap);
        List<DataPoint> collect =
            data.stream()
                .collect(Collectors.groupingBy(keyExtractor, collector))
                .entrySet()
                .stream()
                .map(
                    e -> {
                      DataPoint dataPoint = e.getValue();
                      Map<String, Object> identifiers = e.getKey();
                      for (Map.Entry<String, Object> identifierElement : identifiers.entrySet()) {
                        dataPoint.set(identifierElement.getKey(), identifierElement.getValue());
                      }
                      return dataPoint;
                    })
                .collect(Collectors.toList());

        return new InMemoryDataset(collect, structure);
      }

      @Override
      public DataStructure getDataStructure() {
        return structure;
      }
    };
  }

  @Override
  public DatasetExpression executeSimpleAnalytic(
      DatasetExpression dataset,
      String targetColumnName,
      Analytics.Function function,
      String columnName,
      List<String> partitionBy,
      Map<String, Analytics.Order> orderBy,
      Analytics.WindowSpec window) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatasetExpression executeLeadOrLagAn(
      DatasetExpression dataset,
      String targetColumnName,
      Analytics.Function function,
      String columnName,
      int offset,
      List<String> partitionBy,
      Map<String, Analytics.Order> orderBy) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatasetExpression executeRatioToReportAn(
      DatasetExpression dataset,
      String targetColumnName,
      Analytics.Function function,
      String columnName,
      List<String> partitionBy) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatasetExpression executeRankAn(
      DatasetExpression dataset,
      String targetColumnName,
      Analytics.Function function,
      List<String> partitionBy,
      Map<String, Analytics.Order> orderBy) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatasetExpression executeLeftJoin(
      Map<String, DatasetExpression> datasets, List<Component> components) {
    var iterator = datasets.values().iterator();
    var leftMost = iterator.next();
    while (iterator.hasNext()) {
      leftMost = handleLeftJoin(components, leftMost, iterator.next());
    }
    return leftMost;
  }

  @Override
  public DatasetExpression executeInnerJoin(
      Map<String, DatasetExpression> datasets, List<Component> components) {
    var iterator = datasets.values().iterator();
    var leftMost = iterator.next();
    while (iterator.hasNext()) {
      leftMost = handleInnerJoin(components, leftMost, iterator.next());
    }
    return leftMost;
  }

  @Override
  public DatasetExpression executeCrossJoin(
      Map<String, DatasetExpression> datasets, List<Component> identifiers) {
    var iterator = datasets.values().iterator();
    var leftMost = iterator.next();
    while (iterator.hasNext()) {
      leftMost = handleCrossJoin(identifiers, leftMost, iterator.next());
    }
    return leftMost;
  }

  @Override
  public DatasetExpression executeFullJoin(
      Map<String, DatasetExpression> datasets, List<Component> identifiers) {
    var iterator = datasets.values().iterator();
    var leftMost = iterator.next();
    while (iterator.hasNext()) {
      leftMost = handleFullJoin(identifiers, leftMost, iterator.next());
    }
    return leftMost;
  }

  @Override
  public DatasetExpression executeValidateDPruleset(
      DataPointRuleset dpr,
      DatasetExpression dataset,
      String output,
      Positioned pos,
      List<String> toDrop) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatasetExpression executeValidationSimple(
      DatasetExpression dsE,
      ResolvableExpression erCodeE,
      ResolvableExpression erLevelE,
      DatasetExpression imbalanceE,
      String output,
      Positioned pos) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatasetExpression executeHierarchicalValidation(
      DatasetExpression dsE,
      HierarchicalRuleset hr,
      String componentID,
      String validationMode,
      String inputMode,
      String validationOutput,
      Positioned pos) {
    throw new UnsupportedOperationException();
  }

  /** Returns a structure with the common identifiers only once. */
  private DataStructure createCommonStructure(
      List<Component> identifiers, DatasetExpression left, DatasetExpression right) {
    List<Component> components = new ArrayList<>(identifiers);
    for (Component component : left.getDataStructure().values()) {
      if (!identifiers.contains(component)) {
        components.add(component);
      }
    }
    for (Component component : right.getDataStructure().values()) {
      if (!identifiers.contains(component)) {
        components.add(component);
      }
    }
    return new DataStructure(components);
  }

  /** Creates a datapoint comparator that operates on the given identifiers only. */
  private Comparator<DataPoint> createPredicate(List<Component> identifiers) {
    return (dl, dr) -> {
      for (Component identifier : identifiers) {
        if (!Objects.equals(dl.get(identifier.getName()), dr.get(identifier.getName()))) {
          return -1;
        }
      }
      return 0;
    };
  }

  private DatasetExpression handleInnerJoin(
      List<Component> identifiers, DatasetExpression left, DatasetExpression right) {
    var structure = createCommonStructure(identifiers, left, right);
    var predicate = createPredicate(identifiers);

    return new DatasetExpression(left) {
      @Override
      public Dataset resolve(Map<String, Object> context) {
        var leftPoints = left.resolve(context).getDataPoints();
        var rightPoints = right.resolve(context).getDataPoints();
        List<DataPoint> result = new ArrayList<>();
        for (DataPoint leftPoint : leftPoints) {
          List<DataPoint> matches = new ArrayList<>();
          for (DataPoint rightPoint : rightPoints) {
            // Check equality
            if (predicate.compare(leftPoint, rightPoint) == 0) {
              matches.add(rightPoint);
            }
          }

          if (!matches.isEmpty()) {
            // Create merge datapoint.
            var mergedPoint = new DataPoint(structure);
            for (String leftColumn : left.getDataStructure().keySet()) {
              mergedPoint.set(leftColumn, leftPoint.get(leftColumn));
            }
            for (DataPoint match : matches) {
              var matchPoint = new DataPoint(structure, mergedPoint);
              for (String rightColumn : right.getDataStructure().keySet()) {
                matchPoint.set(rightColumn, match.get(rightColumn));
              }
              result.add(matchPoint);
            }
          }
        }
        return new InMemoryDataset(result, structure);
      }

      @Override
      public DataStructure getDataStructure() {
        return structure;
      }
    };
  }

  private DatasetExpression handleFullJoin(
      List<Component> identifiers, DatasetExpression left, DatasetExpression right) {
    // Naive implementation, left and right union. Could be optimized.
    return executeUnion(
        List.of(
            handleLeftJoin(identifiers, left, right), handleLeftJoin(identifiers, right, left)));
  }

  private DatasetExpression handleLeftJoin(
      List<Component> identifiers, DatasetExpression left, DatasetExpression right) {
    var structure = createCommonStructure(identifiers, left, right);
    var predicate = createPredicate(identifiers);

    return new DatasetExpression(left) {
      @Override
      public Dataset resolve(Map<String, Object> context) {
        var leftPoints = left.resolve(context).getDataPoints();
        var rightPoints = right.resolve(context).getDataPoints();
        List<DataPoint> result = new ArrayList<>();
        for (DataPoint leftPoint : leftPoints) {
          List<DataPoint> matches = new ArrayList<>();
          for (DataPoint rightPoint : rightPoints) {
            // Check equality
            if (predicate.compare(leftPoint, rightPoint) == 0) {
              matches.add(rightPoint);
            }
          }

          // Create merge datapoint.
          var mergedPoint = new DataPoint(structure);
          for (String leftColumn : left.getDataStructure().keySet()) {
            mergedPoint.set(leftColumn, leftPoint.get(leftColumn));
          }

          if (matches.isEmpty()) {
            result.add(mergedPoint);
          } else {
            for (DataPoint match : matches) {
              var matchPoint = new DataPoint(structure, mergedPoint);
              for (String rightColumn : right.getDataStructure().keySet()) {
                matchPoint.set(rightColumn, match.get(rightColumn));
              }
              result.add(matchPoint);
            }
          }
        }
        return new InMemoryDataset(result, structure);
      }

      @Override
      public DataStructure getDataStructure() {
        return structure;
      }
    };
  }

  private DatasetExpression handleCrossJoin(
      List<Component> identifiers, DatasetExpression left, DatasetExpression right) {
    var structure = createCommonStructure(identifiers, left, right);
    return new DatasetExpression(left) {
      @Override
      public Dataset resolve(Map<String, Object> context) {
        var leftPoints = left.resolve(context).getDataPoints();
        var rightPoints = right.resolve(context).getDataPoints();
        List<DataPoint> result = new ArrayList<>();
        // Nested-loop implementation
        for (DataPoint leftPoint : leftPoints) {
          for (DataPoint rightPoint : rightPoints) {
            var mergedPoint = new DataPoint(structure);
            for (String leftColumn : left.getDataStructure().keySet()) {
              mergedPoint.set(leftColumn, leftPoint.get(leftColumn));
            }
            for (String rightColumn : right.getDataStructure().keySet()) {
              mergedPoint.set(rightColumn, rightPoint.get(rightColumn));
            }
            result.add(mergedPoint);
          }
        }
        return new InMemoryDataset(result, structure);
      }

      @Override
      public DataStructure getDataStructure() {
        return structure;
      }
    };
  }

  @Override
  public DatasetExpression executePivot(
      DatasetExpression dataset, String idName, String meName, Positioned pos) {
    throw new UnsupportedOperationException();
  }

  /**
   * The <code>Factory</code> class is an implementation of a VTL engine factory that returns
   * in-memory engines.
   */
  public static class Factory implements ProcessingEngineFactory {

    @Override
    public String getName() {
      return "memory";
    }

    @Override
    public ProcessingEngine getProcessingEngine(ScriptEngine engine) {
      return new InMemoryProcessingEngine();
    }
  }
}
