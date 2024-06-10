package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.expressions.ComponentExpression;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.*;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.model.utils.Java8Helpers;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import org.antlr.v4.runtime.tree.ParseTree;
import org.threeten.extra.Interval;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

/**
 * <code>TimeFunctionsVisitor</code> is the base visitor for expressions involving time functions.
 */
public class TimeFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final GenericFunctionsVisitor genericFunctionsVisitor;
    private final ExpressionVisitor expressionVisitor;
    private final ProcessingEngine processingEngine;

    public TimeFunctionsVisitor(GenericFunctionsVisitor genericFunctionsVisitor, ExpressionVisitor expressionVisitor, ProcessingEngine processingEngine) {
        this.genericFunctionsVisitor = genericFunctionsVisitor;
        this.expressionVisitor = expressionVisitor;
        this.processingEngine = processingEngine;
    }

    /**
     * Visits the current date expression.
     *
     * @param ctx The scripting context for the expression (left and right expressions should be the string parameters).
     * @return A <code>ResolvableExpression</code> resolving to a long integer representing the Levenshtein distance between the parameters.
     */
    @Override
    public ResolvableExpression visitCurrentDateAtom(VtlParser.CurrentDateAtomContext ctx) {
        return new ConstantExpression(Instant.now(), fromContext(ctx));
    }

    @Override
    public ResolvableExpression visitFlowAtom(VtlParser.FlowAtomContext ctx) {
        if (ctx.FLOW_TO_STOCK() != null) {
            return flowToStock(ctx);
        } else if (ctx.STOCK_TO_FLOW() != null) {
            return stockToFlows(ctx);
        } else {
            throw new UnsupportedOperationException("unknown op token " + ctx.op);
        }
    }

    private ResolvableExpression stockToFlows(VtlParser.FlowAtomContext ctx) {
        VtlParser.ExprContext expr = ctx.expr();
        ResolvableExpression operand = expressionVisitor.visit(expr);

        try {

            // Fall through if not dataset.
            Positioned position = fromContext(ctx);
            if (!(operand instanceof DatasetExpression)) {
                throw new InvalidArgumentException("flow to stock only supports datasets", position);
            }

            DatasetExpression ds = (DatasetExpression) operand;

            LinkedHashMap<String, Analytics.Order> ids = ds.getIdentifiers().stream()
                    .collect(Collectors.toMap(
                            Structured.Component::getName,
                            c -> Analytics.Order.ASC,
                            (a, b) -> b,
                            LinkedHashMap::new

                    ));
            Structured.Component time = extractTimeComponent(ctx, ds);
            List<String> partition = ids.keySet().stream()
                    .filter(colName -> !time.getName().equals(colName))
                    .collect(Collectors.toList());
            for (Structured.Component measure : ds.getMeasures()) {

                if (!Number.class.isAssignableFrom(measure.getType())) {
                    continue;
                }

                String measureName = measure.getName();
                String lagColumnName = measure.getName() + "_lag";

                DatasetExpression lag = processingEngine.executeLeadOrLagAn(ds, measureName, Analytics.Function.LAG, measureName, 1, partition, ids);
                lag = processingEngine.executeRename(lag, Java8Helpers.mapOf(measureName, lagColumnName));

                lag = processingEngine.executeProject(lag,
                        Stream.concat(ds.getIdentifiers().stream().map(Structured.Component::getName), Stream.of(lagColumnName)).collect(Collectors.toList())
                );

                ds = processingEngine.executeLeftJoin(Java8Helpers.mapOf("left", ds, "lag", lag), ds.getIdentifiers());

                // me - nvl(lag, 0)
                ComponentExpression measureExpr = new ComponentExpression(ds.getDataStructure().get(measure.getName()), position);
                ComponentExpression lagExpr = new ComponentExpression(ds.getDataStructure().get(lagColumnName), position);
                ResolvableExpression nvlExpr = genericFunctionsVisitor.invokeFunction("nvl", Java8Helpers.listOf(
                        lagExpr, new ConstantExpression(0L, position)), position);
                ResolvableExpression subtractionExpr = genericFunctionsVisitor.invokeFunction("subtraction", Java8Helpers.listOf(
                        measureExpr, nvlExpr
                ), position);

                ds = processingEngine.executeCalc(ds, Java8Helpers.mapOf(measure.getName(), subtractionExpr), Java8Helpers.mapOf(), Java8Helpers.mapOf());
                ds = processingEngine.executeProject(ds, ds.getColumnNames().stream().filter(s -> !s.equals(lagColumnName)).collect(Collectors.toList()));
            }
            return ds;
        } catch (VtlScriptException iae) {
            throw new VtlRuntimeException(iae);
        }

    }

    private ResolvableExpression flowToStock(VtlParser.FlowAtomContext ctx) {
        VtlParser.ExprContext expr = ctx.expr();
        ResolvableExpression operand = expressionVisitor.visit(expr);

        try {

            // Fall through if not dataset.
            Positioned position = fromContext(ctx);
            if (!(operand instanceof DatasetExpression)) {
                throw new InvalidArgumentException("flow to stock only supports datasets", position);
            }

            DatasetExpression ds = (DatasetExpression) operand;
            LinkedHashMap<String, Analytics.Order> ids = ds.getIdentifiers().stream()
                    .collect(Collectors.toMap(
                            Structured.Component::getName,
                            c -> Analytics.Order.ASC,
                            (a, b) -> b,
                            LinkedHashMap::new

                    ));
            Structured.Component time = extractTimeComponent(ctx, ds);
            List<String> partition = ids.keySet().stream()
                    .filter(colName -> !time.getName().equals(colName))
                    .collect(Collectors.toList());
            for (Structured.Component measure : ds.getMeasures()) {
                ds = processingEngine.executeSimpleAnalytic(ds, measure.getName(), Analytics.Function.SUM,
                        measure.getName(), partition, ids, null);
            }
            return ds;
        } catch (VtlScriptException iae) {
            throw new VtlRuntimeException(iae);
        }
    }

    @Override
    public ResolvableExpression visitTimeShiftAtom(VtlParser.TimeShiftAtomContext ctx) {
        try {
            // signed integer is a special rule, so we cannot rely on the expression visitor. This means that the
            // second parameter must be a signed integer literal.
            ResolvableExpression operand = expressionVisitor.visit(ctx.expr());
            ConstantExpression n = new ConstantExpression(Long.parseLong(ctx.signedInteger().getText()), fromContext(ctx.signedInteger()));

            // Fall through if not dataset.
            if (!(operand instanceof DatasetExpression)) {
                return genericFunctionsVisitor.invokeFunction("timeshift", Java8Helpers.listOf(
                        operand,
                        n
                ), fromContext(ctx));
            }

            DatasetExpression ds = (DatasetExpression) operand;

            Structured.Component t = extractTimeComponent(ctx, ds);

            ResolvableExpression compExpr = genericFunctionsVisitor.invokeFunction("timeshift", Java8Helpers.listOf(
                    new ComponentExpression(t, fromContext(ctx)),
                    n
            ), fromContext(ctx));
            return processingEngine.executeCalc(ds, Java8Helpers.mapOf(t.getName(), compExpr), Java8Helpers.mapOf(t.getName(), t.getRole()), Java8Helpers.mapOf());

        } catch (VtlScriptException e) {
            throw new VtlRuntimeException(e);
        }
    }

    private static Structured.Component extractTimeComponent(ParseTree ctx, DatasetExpression ds) throws InvalidArgumentException {
        Structured.Component t = ds.getIdentifiers().stream()
                .filter(component -> component.getType().equals(Interval.class)
                        || component.getType().equals(Instant.class)
                        || component.getType().equals(ZonedDateTime.class)
                        || component.getType().equals(OffsetDateTime.class))
                .findFirst()
                .orElseThrow(() -> new InvalidArgumentException("no time column in " + ctx.getText(), fromContext(ctx)));
        return t;
    }
}
