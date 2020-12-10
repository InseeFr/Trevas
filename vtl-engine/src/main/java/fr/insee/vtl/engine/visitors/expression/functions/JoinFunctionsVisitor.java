package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import org.antlr.v4.runtime.RuleContext;

import java.util.*;
import java.util.stream.Collectors;

import static fr.insee.vtl.engine.utils.TypeChecking.assertTypeExpression;
import static fr.insee.vtl.model.Dataset.*;

public class JoinFunctionsVisitor extends VtlBaseVisitor<DatasetExpression> {

    private final ExpressionVisitor expressionVisitor;

    public JoinFunctionsVisitor(ExpressionVisitor expressionVisitor) {
        this.expressionVisitor = Objects.requireNonNull(expressionVisitor);
    }

    private static List<Component> findCommonIdentifiers(Collection<DatasetExpression> datasetExpressions) {
        Set<Component> intersection = new LinkedHashSet<>();
        for (DatasetExpression datasetExpression : datasetExpressions) {
            var structure = datasetExpression.getDataStructure();
            if (intersection.isEmpty()) {
                for (Component component : structure.values()) {
                    if (Role.IDENTIFIER.equals(component.getRole())) {
                        intersection.add(component);
                    }
                }
            } else {
                intersection.retainAll(structure.values());
            }
        }
        return new ArrayList<>(intersection);
    }

    private static Optional<List<Component>> checkSameIdentifiers(Collection<DatasetExpression> datasetExpressions) {
        Set<Set<Component>> identifiers = new HashSet<>();
        for (DatasetExpression datasetExpression : datasetExpressions) {
            var structure = datasetExpression.getDataStructure();
            var ids = new HashSet<Component>();
            for (Component component : structure.values()) {
                if (component.getRole().equals(Role.IDENTIFIER)) {
                    ids.add(component);
                }
            }
            identifiers.add(ids);
        }
        if (identifiers.size() != 1) {
            return Optional.empty();
        } else {
            return Optional.of(new ArrayList<>(identifiers.iterator().next()));
        }
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

        // Left join require that all the datasets have the same identifiers.
        var commonIdentifiers = checkSameIdentifiers(datasets.values())
                .orElseThrow(() -> new VtlRuntimeException(
                        new InvalidArgumentException("datasets must have common identifiers", joinClauseContext)
                ));

        // Remove the identifiers
        if (joinClauseContext.USING() != null) {
            var identifierNames = commonIdentifiers.stream()
                    .map(Component::getName)
                    .collect(Collectors.toList());
            var usingNames = new ArrayList<String>();
            for (VtlParser.ComponentIDContext usingContext : joinClauseContext.componentID()) {
                var name = usingContext.getText();
                if (!identifierNames.contains(name)) {
                    throw new VtlRuntimeException(
                            new InvalidArgumentException("not in the set of common identifiers", usingContext)
                    );
                }
                usingNames.add(name);
            }
            commonIdentifiers.removeIf(component -> !usingNames.contains(component.getName()));
        }

        var iterator = datasets.values().iterator();
        var leftMost = iterator.next();
        while (iterator.hasNext()) {
            leftMost = handleLeftJoin(commonIdentifiers, leftMost, iterator.next());
        }
        return leftMost;
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
