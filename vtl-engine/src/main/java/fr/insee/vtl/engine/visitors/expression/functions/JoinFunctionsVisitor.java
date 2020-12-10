package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import org.antlr.v4.runtime.RuleContext;

import java.util.*;
import java.util.stream.Collectors;

import static fr.insee.vtl.engine.utils.TypeChecking.assertTypeExpression;
import static fr.insee.vtl.model.Structured.*;

public class JoinFunctionsVisitor extends VtlBaseVisitor<DatasetExpression> {

    private final ExpressionVisitor expressionVisitor;

    public JoinFunctionsVisitor(ExpressionVisitor expressionVisitor) {
        this.expressionVisitor = Objects.requireNonNull(expressionVisitor);
    }

    @Override
    public DatasetExpression visitJoinExpr(VtlParser.JoinExprContext ctx) {
        if (ctx.LEFT_JOIN() != null) {
            return leftJoin(ctx);
        } else if (ctx.INNER_JOIN() != null) {
            return innerJoin(ctx);
        } else if (ctx.FULL_JOIN() != null) {
            return fullJoin(ctx);
        } else if (ctx.CROSS_JOIN() != null) {
            return crossJoin(ctx);
        }
        throw new UnsupportedOperationException("unknown join type");
    }

    /**
     * Normalize the
     *
     * @param joinClauseContext
     * @return
     */
    private LinkedHashMap<String, DatasetExpression> normalizeDatasets(VtlParser.JoinClauseContext joinClauseContext) {
        LinkedHashMap<String, DatasetExpression> datasets = new LinkedHashMap<>();
        for (VtlParser.JoinClauseItemContext joinClauseItem : joinClauseContext.joinClauseItem()) {
            // Expression here is not so nice if alias is optional.. We force var id.
            var datasetExpressionContext = joinClauseItem.expr();
            if (!(datasetExpressionContext instanceof VtlParser.VarIdExprContext)) {
                throw new VtlRuntimeException(
                        new InvalidArgumentException("use a variable", datasetExpressionContext));
            }
            var alias = Optional.<RuleContext>ofNullable(joinClauseItem.alias())
                    .orElse(datasetExpressionContext).getText();
            var datasetExpression = (DatasetExpression) assertTypeExpression(
                    expressionVisitor.visit(datasetExpressionContext),
                    Dataset.class, datasetExpressionContext);
            datasets.put(alias, datasetExpression);
        }
        return datasets;
    }

    private DatasetExpression leftJoin(VtlParser.JoinExprContext ctx) {
        var joinClauseContext = ctx.joinClause();
        var datasets = normalizeDatasets(joinClauseContext);

        // TODO: support "using"
        // Build a list of common identifier (type and name) in all the datasets
        var identfiers = datasets.values().stream()
                .map(Structured::getDataStructure)
                .flatMap(structure -> structure.values().stream())
                .filter(component -> Dataset.Role.IDENTIFIER.equals(component.getRole()))
                .distinct()
                .collect(Collectors.toList());

        if (identfiers.isEmpty()) {
            throw new VtlRuntimeException(
                    new InvalidArgumentException("datasets must have common identifiers", joinClauseContext)
            );
        }

        var iterator = datasets.values().iterator();
        DatasetExpression result = handleLeftJoin(identfiers, iterator.next(), iterator.next());
        return result;
    }

    private DatasetExpression handleLeftJoin(List<Component> identifiers, DatasetExpression left, DatasetExpression right) {
        // Create common structure
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

        var structure = new DataStructure(components);

        // Predicate for the join. Could be using the
        Comparator<DataPoint> predicate = (dl, dr) -> {
            for (Component identifier : identifiers) {
                if (!Objects.equals(dl.get(identifier.getName()), dr.get(identifier.getName()))) {
                    return -1;
                }
            }
            return 0;
        };

        return new DatasetExpression() {
            @Override
            public Dataset resolve(Map<String, Object> context) {
                var leftPoints = left.resolve(context).getDataPoints();
                var rightPoints = right.resolve(context).getDataPoints();
                List<DataPoint> result = new ArrayList<>();
                for (DataPoint leftPoint : leftPoints) {
                    // Create merge datapoint.
                    var mergedPoint = new DataPoint(structure);
                    for (String leftColumn : left.getDataStructure().keySet()) {
                        mergedPoint.set(leftColumn, leftPoint.get(leftColumn));
                    }

                    for (DataPoint rightPoint : rightPoints) {
                        // Check equality
                        if (predicate.compare(leftPoint, rightPoint) == 0) {
                            for (String rightColumn : right.getDataStructure().keySet()) {
                                mergedPoint.set(rightColumn, rightPoint.get(rightColumn));
                            }
                        }
                    }
                    result.add(mergedPoint);
                }
                return new InMemoryDataset(result, structure);
            }

            @Override
            public DataStructure getDataStructure() {
                return structure;
            }
        };
    }

    private DatasetExpression crossJoin(VtlParser.JoinExprContext ctx) {
        throw new UnsupportedOperationException("TODO: cross_join");
    }

    private DatasetExpression fullJoin(VtlParser.JoinExprContext ctx) {
        throw new UnsupportedOperationException("TODO: full_join");
    }

    private DatasetExpression innerJoin(VtlParser.JoinExprContext ctx) {
        throw new UnsupportedOperationException("TODO: inner_join");
    }


}
