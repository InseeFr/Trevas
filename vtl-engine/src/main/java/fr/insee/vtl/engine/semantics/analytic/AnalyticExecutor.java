package fr.insee.vtl.engine.semantics.analytic;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

import fr.insee.vtl.antlr.runtime.Token;
import fr.insee.vtl.antlr.runtime.tree.ParseTree;
import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.ClauseVisitor;
import fr.insee.vtl.model.Analytics;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlParser;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Clause- and mono-measure analytic orchestration on {@link ProcessingEngine}. */
public final class AnalyticExecutor {

  private AnalyticExecutor() {}

  public static DatasetExpression execute(
      ParseTree ctx, ProcessingEngine engine, DatasetExpression dataset, String targetColumnName) {
    if (ctx instanceof VtlParser.AnSimpleFunctionContext c) {
      return executeSimple(c, engine, dataset, targetColumnName);
    }
    if (ctx instanceof VtlParser.LagOrLeadAnContext c) {
      return executeLagLead(c, engine, dataset, targetColumnName);
    }
    if (ctx instanceof VtlParser.RatioToReportAnContext c) {
      return executeRatioToReport(c, engine, dataset, targetColumnName);
    }
    if (ctx instanceof VtlParser.RankAnContext c) {
      return executeRank(c, engine, dataset, targetColumnName);
    }
    if (ctx instanceof VtlParser.AnalyticFunctionsContext c) {
      return execute(c.anFunction(), engine, dataset, targetColumnName);
    }
    throw new VtlRuntimeException(
        new InvalidArgumentException("not an analytic function", fromContext(ctx)));
  }

  public static DatasetExpression executeSimple(
      VtlParser.AnSimpleFunctionContext ctx,
      ProcessingEngine engine,
      DatasetExpression dataset,
      String targetColumnName) {
    return engine.executeSimpleAnalytic(
        dataset,
        targetColumnName,
        toFunction(ctx.op, ctx),
        ctx.expr().getText(),
        toPartitionBy(ctx.partition),
        toOrderBy(ctx.orderBy),
        toWindowSpec(ctx.windowing));
  }

  public static DatasetExpression executeLagLead(
      VtlParser.LagOrLeadAnContext ctx,
      ProcessingEngine engine,
      DatasetExpression dataset,
      String targetColumnName) {
    return engine.executeLeadOrLagAn(
        dataset,
        targetColumnName,
        toFunction(ctx.op, ctx),
        ctx.expr().getText(),
        Integer.parseInt(ctx.offset.getText()),
        toPartitionBy(ctx.partition),
        toOrderBy(ctx.orderBy));
  }

  public static DatasetExpression executeRatioToReport(
      VtlParser.RatioToReportAnContext ctx,
      ProcessingEngine engine,
      DatasetExpression dataset,
      String targetColumnName) {
    return engine.executeRatioToReportAn(
        dataset,
        targetColumnName,
        toFunction(ctx.op, ctx),
        ctx.expr().getText(),
        toPartitionBy(ctx.partition));
  }

  public static DatasetExpression executeRank(
      VtlParser.RankAnContext ctx,
      ProcessingEngine engine,
      DatasetExpression dataset,
      String targetColumnName) {
    return engine.executeRankAn(
        dataset,
        targetColumnName,
        toFunction(ctx.op, ctx),
        toPartitionBy(ctx.partition),
        toOrderBy(ctx.orderBy));
  }

  private static Analytics.Function toFunction(Token op, ParseTree ctx) {
    return switch (op.getType()) {
      case VtlParser.SUM -> Analytics.Function.SUM;
      case VtlParser.AVG -> Analytics.Function.AVG;
      case VtlParser.COUNT -> Analytics.Function.COUNT;
      case VtlParser.MEDIAN -> Analytics.Function.MEDIAN;
      case VtlParser.MIN -> Analytics.Function.MIN;
      case VtlParser.MAX -> Analytics.Function.MAX;
      case VtlParser.STDDEV_POP -> Analytics.Function.STDDEV_POP;
      case VtlParser.STDDEV_SAMP -> Analytics.Function.STDDEV_SAMP;
      case VtlParser.VAR_POP -> Analytics.Function.VAR_POP;
      case VtlParser.VAR_SAMP -> Analytics.Function.VAR_SAMP;
      case VtlParser.FIRST_VALUE -> Analytics.Function.FIRST_VALUE;
      case VtlParser.LAST_VALUE -> Analytics.Function.LAST_VALUE;
      case VtlParser.LEAD -> Analytics.Function.LEAD;
      case VtlParser.LAG -> Analytics.Function.LAG;
      case VtlParser.RATIO_TO_REPORT -> Analytics.Function.RATIO_TO_REPORT;
      case VtlParser.RANK -> Analytics.Function.RANK;
      default ->
          throw new VtlRuntimeException(
              new InvalidArgumentException("not an analytic function", fromContext(ctx)));
    };
  }

  private static List<String> toPartitionBy(VtlParser.PartitionByClauseContext partition) {
    if (partition == null) {
      return List.of();
    }
    return partition.componentID().stream()
        .map(ClauseVisitor::getName)
        .collect(Collectors.toList());
  }

  private static Map<String, Analytics.Order> toOrderBy(VtlParser.OrderByClauseContext orderByCtx) {
    if (orderByCtx == null) {
      return Map.of();
    }
    Map<String, Analytics.Order> orderBy = new LinkedHashMap<>();
    for (VtlParser.OrderByItemContext item : orderByCtx.orderByItem()) {
      String columnName = ClauseVisitor.getName(item.componentID());
      orderBy.put(columnName, item.DESC() != null ? Analytics.Order.DESC : Analytics.Order.ASC);
    }
    return orderBy;
  }

  private static Analytics.WindowSpec toWindowSpec(VtlParser.WindowingClauseContext windowing) {
    if (windowing == null) {
      return null;
    }
    Long from = toRangeLong(windowing.from_);
    Long to = toRangeLong(windowing.to_);
    if (windowing.RANGE() != null) {
      return new Analytics.RangeWindow(from, to);
    }
    return new Analytics.DataPointWindow(from, to);
  }

  private static Long toRangeLong(VtlParser.LimitClauseItemContext ctx) {
    if (ctx.CURRENT() != null) {
      return 0L;
    }
    if (ctx.UNBOUNDED() != null && ctx.PRECEDING() != null) {
      return Long.MIN_VALUE;
    }
    if (ctx.UNBOUNDED() != null && ctx.FOLLOWING() != null) {
      return Long.MAX_VALUE;
    }
    if (ctx.INTEGER_CONSTANT() != null) {
      return Long.parseLong(ctx.getChild(0).getText());
    }
    throw new VtlRuntimeException(new VtlScriptException("invalid range", fromContext(ctx)));
  }
}
