package fr.insee.vtl.engine.visitors;

import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.exceptions.VtlScriptException;
import fr.insee.vtl.engine.visitors.expression.ConstantVisitor;
import fr.insee.vtl.model.Analytics;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.LongExpression;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AnalyticsVisitor extends VtlBaseVisitor<DatasetExpression> {

    private final ProcessingEngine processingEngine;
    private final DatasetExpression dataset;

    public AnalyticsVisitor(ProcessingEngine processingEngine, DatasetExpression dataset) {
        this.processingEngine = processingEngine;
        this.dataset = dataset;
    }

    /**
     * Convert the analytic expression function name from token type to Enum type Analytics.Function
     * @param op The function name of the analytic expression with type token
     * @param ctx The context of the parse tree
     * @return The function name of the analytic expression with Enum type Analytics.Function
     */
    private Analytics.Function toFunctionEnum(Token op, ParseTree ctx) {
        switch (op.getType()) {
            case VtlParser.SUM:
                return Analytics.Function.SUM;
            case VtlParser.AVG:
                return Analytics.Function.AVG;
            case VtlParser.COUNT:
                return Analytics.Function.COUNT;
            case VtlParser.MEDIAN:
                return Analytics.Function.MEDIAN;
            case VtlParser.MIN:
                return Analytics.Function.MIN;
            case VtlParser.MAX:
                return Analytics.Function.MAX;
            case VtlParser.STDDEV_POP:
                return Analytics.Function.STDDEV_POP;
            case VtlParser.STDDEV_SAMP:
                return Analytics.Function.STDDEV_SAMP;
            case VtlParser.VAR_POP:
                return Analytics.Function.VAR_POP;
            case VtlParser.VAR_SAMP:
                return Analytics.Function.VAR_SAMP;
            case VtlParser.FIRST_VALUE:
                return Analytics.Function.FIRST_VALUE;
            case VtlParser.LAST_VALUE:
                return Analytics.Function.LAST_VALUE;
            case VtlParser.LEAD:
                return Analytics.Function.LEAD;
            case VtlParser.LAG:
                return Analytics.Function.LAG;
            case VtlParser.RATIO_TO_REPORT:
                return Analytics.Function.RATIO_TO_REPORT;
            default:
                throw new VtlRuntimeException(
                        new InvalidArgumentException("not an analytic function", ctx)
                );
        }
    }

    /**
     *  Convert the partitionByClause to a list of <colName> which the dataset are partitionBy
     * @param partition The parse tree context of PartitionByClause
     * @return a list of <colName> which the dataset are partitionBy
     */
    private List<String> toPartitionBy(VtlParser.PartitionByClauseContext partition) {
        if (partition == null) {
            return null;
        }
        return partition.componentID().stream()
                .map(ClauseVisitor::getName)
                .collect(Collectors.toList());
    }

    /**
     * Convert the orderByClause to a pair of <colName,order(e.g. asc, desc)>
     * @param orderByCtx The parse tree context of OrderByClause
     * @return a map of <colName,order(e.g. asc, desc)> which the dataset are orderedBy
     */
    private Map<String, Analytics.Order> toOrderBy(VtlParser.OrderByClauseContext orderByCtx) {
        if (orderByCtx == null) {
            return null;
        }
        Map<String, Analytics.Order> orderBy = new LinkedHashMap<>();
        for (VtlParser.OrderByItemContext item : orderByCtx.orderByItem()) {
            String columnName = ClauseVisitor.getName(item.componentID());
            if (item.DESC() != null) {
                orderBy.put(columnName, Analytics.Order.DESC);
            } else {
                orderBy.put(columnName, Analytics.Order.ASC);
            }
        }
        return orderBy;
    }

    /**
     * Convert the windowSpec clause expression to a class WindowSpec (e.g. datapoints or range)
     * @param windowing the parse tree context of window frame
     * @return an object of windowSpec class that will be RangeWindow(from,to) or DataPointWindow(from, to)
     */
    private Analytics.WindowSpec toWindowSpec(VtlParser.WindowingClauseContext windowing) {
        if (windowing == null) {
            return null;
        }
        Long from = toRangeLong(windowing.from_);
        Long to = toRangeLong(windowing.to_);
        if (windowing.RANGE() != null) {
            return new Analytics.RangeWindow(from, to);
        } else {
            return new Analytics.DataPointWindow(from, to);
        }
    }


    /**
     * Convert the range expression to Long.
     * Note in vtl the from and to of the range can be reversed. For example: data points between -2 following and -2 preceding
     * is correct
     * @param ctx the parse tree context of window range clause
     * @return long
     */

    private Long toRangeLong(VtlParser.LimitClauseItemContext ctx) {
        if (ctx.CURRENT() != null) {
            return 0L;
        } else if (ctx.UNBOUNDED() != null && ctx.PRECEDING() != null) {
            return Long.MIN_VALUE;
        } else if (ctx.UNBOUNDED() != null && ctx.FOLLOWING() != null) {
            return Long.MAX_VALUE;
        } else if (ctx.INTEGER_CONSTANT() != null) {
            return Long.parseLong(ctx.getChild(0).getText());
        }
        throw new VtlRuntimeException(new VtlScriptException("invalid range", ctx));
    }

    private String toTargetColName(VtlParser.ExprContext expr){
        return expr.getText();
    }

    private int toOffset(VtlParser.SignedIntegerContext offet) {
        return Integer.parseInt(offet.getText());
    }

    @Override
    public DatasetExpression visitAnSimpleFunction(VtlParser.AnSimpleFunctionContext ctx) {

        return processingEngine.executeSimpleAnalytic(
                dataset,
                toFunctionEnum(ctx.op, ctx),
                toTargetColName(ctx.expr()),
                toPartitionBy(ctx.partition),
                toOrderBy(ctx.orderBy),
                toWindowSpec(ctx.windowing)
        );
    }

    @Override
    public DatasetExpression visitLagOrLeadAn(VtlParser.LagOrLeadAnContext ctx) {
        return processingEngine.executeLeadOrLagAn(
                dataset,
                toFunctionEnum(ctx.op, ctx),
                toTargetColName(ctx.expr()),
                toOffset(ctx.offet),
                toPartitionBy(ctx.partition),
                toOrderBy(ctx.orderBy)

        );
    }



    @Override
    public DatasetExpression visitRatioToReportAn(VtlParser.RatioToReportAnContext ctx) {
        return processingEngine.executeRatioToReportAn(
                dataset,
                toFunctionEnum(ctx.op, ctx),
                toTargetColName(ctx.expr()),
                toPartitionBy(ctx.partition)


        );
    }
}
