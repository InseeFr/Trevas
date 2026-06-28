package fr.insee.vtl.engine.semantics.time;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

import fr.insee.vtl.antlr.runtime.tree.ParseTree;
import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.expressions.ComponentExpression;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.engine.visitors.expression.functions.GenericFunctionsVisitor;
import fr.insee.vtl.model.*;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlParser;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.threeten.extra.Interval;

/** VTL time operators ({@code current_date}, {@code flow_to_stock}, {@code timeshift}, …). */
public final class TimeExecutor {

  private TimeExecutor() {}

  public static ResolvableExpression currentDate(VtlParser.CurrentDateAtomContext ctx) {
    return new ConstantExpression(Instant.now(), fromContext(ctx));
  }

  public static DatasetExpression flowToStock(
      VtlParser.FlowAtomContext ctx, ExpressionVisitor expressionVisitor, ProcessingEngine engine)
      throws VtlScriptException {
    Positioned position = fromContext(ctx);
    DatasetExpression dataset = requireDataset(expressionVisitor.visit(ctx.expr()), position);
    return flowToStock(engine, dataset, requireTimeColumn(ctx, dataset));
  }

  public static DatasetExpression stockToFlow(
      VtlParser.FlowAtomContext ctx,
      ExpressionVisitor expressionVisitor,
      ProcessingEngine engine,
      GenericFunctionsVisitor genericFunctions)
      throws VtlScriptException {
    Positioned position = fromContext(ctx);
    DatasetExpression dataset = requireDataset(expressionVisitor.visit(ctx.expr()), position);
    return stockToFlow(
        engine, genericFunctions, dataset, requireTimeColumn(ctx, dataset), position);
  }

  public static ResolvableExpression timeShift(
      VtlParser.TimeShiftAtomContext ctx,
      ExpressionVisitor expressionVisitor,
      ProcessingEngine engine,
      GenericFunctionsVisitor genericFunctions)
      throws VtlScriptException {
    ResolvableExpression operand = expressionVisitor.visit(ctx.expr());
    long offset = Long.parseLong(ctx.signedInteger().getText());
    Positioned position = fromContext(ctx);
    if (!(operand instanceof DatasetExpression ds)) {
      return genericFunctions.invokeFunction(
          "timeshift", List.of(operand, new ConstantExpression(offset, position)), position);
    }
    return timeShift(engine, genericFunctions, ds, requireTimeColumn(ctx, ds), offset, position);
  }

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

  public static DatasetExpression timeShift(
      ProcessingEngine engine,
      GenericFunctionsVisitor genericFunctions,
      DatasetExpression dataset,
      Structured.Component timeColumn,
      long offset,
      Positioned position)
      throws VtlScriptException {
    var compExpr =
        genericFunctions.invokeFunction(
            "timeshift",
            List.of(
                new ComponentExpression(timeColumn, position),
                new ConstantExpression(offset, position)),
            position);
    return engine.executeCalc(
        dataset,
        Map.of(timeColumn.getName(), compExpr),
        Map.of(timeColumn.getName(), timeColumn.getRole()),
        Map.of());
  }

  private static DatasetExpression requireDataset(ResolvableExpression operand, Positioned position)
      throws InvalidArgumentException {
    if (operand instanceof DatasetExpression ds) {
      return ds;
    }
    throw new InvalidArgumentException(
        "time operators on datasets only support datasets", position);
  }

  private static Structured.Component requireTimeColumn(ParseTree ctx, DatasetExpression ds)
      throws InvalidArgumentException {
    return ds.getIdentifiers().stream()
        .filter(TimeExecutor::isTimeColumn)
        .findFirst()
        .orElseThrow(
            () ->
                new InvalidArgumentException(
                    "no time column in " + ctx.getText(), fromContext(ctx)));
  }

  private static boolean isTimeColumn(Structured.Component component) {
    Class<?> type = component.getType();
    return type.equals(Interval.class)
        || type.equals(Instant.class)
        || type.equals(ZonedDateTime.class)
        || type.equals(OffsetDateTime.class);
  }
}
