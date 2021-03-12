package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.*;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import org.antlr.v4.runtime.RuleContext;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static fr.insee.vtl.engine.utils.TypeChecking.assertTypeExpression;
import static fr.insee.vtl.model.Dataset.Component;
import static fr.insee.vtl.model.Dataset.Role;

public class JoinFunctionsVisitor extends VtlBaseVisitor<DatasetExpression> {

    private final ExpressionVisitor expressionVisitor;
    private final ProcessingEngine processingEngine;

    public JoinFunctionsVisitor(ExpressionVisitor expressionVisitor, ProcessingEngine processingEngine) {
        this.expressionVisitor = Objects.requireNonNull(expressionVisitor);
        this.processingEngine = Objects.requireNonNull(processingEngine);
    }

    private static Optional<List<Component>> checkSameIdentifiers(Collection<DatasetExpression> datasetExpressions) {
        Set<Set<Component>> identifiers = new LinkedHashSet<>();
        for (DatasetExpression datasetExpression : datasetExpressions) {
            var structure = datasetExpression.getDataStructure();
            var ids = new LinkedHashSet<Component>();
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

    private LinkedHashMap<String, DatasetExpression> normalizeDatasets(List<VtlParser.JoinClauseItemContext> joinClauseItems) {
        LinkedHashMap<String, DatasetExpression> datasets = new LinkedHashMap<>();
        for (VtlParser.JoinClauseItemContext joinClauseItem : joinClauseItems) {

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

    /**
     * Rename all the components to avoid duplicates.
     */
    private Map<String, DatasetExpression> renameDuplicates(List<Component> identifiers,
                                                                      Map<String, DatasetExpression> datasets) {
        Set<String> identifierNames = identifiers.stream().map(Component::getName).collect(Collectors.toSet());
        Set<String> duplicates = new LinkedHashSet<>();
        Set<String> uniques = new LinkedHashSet<>();
        for (DatasetExpression dataset : datasets.values()) {
            for (String name : dataset.getColumnNames()) {
                // Ignore identifiers.
                if (identifierNames.contains(name)) {
                    continue;
                }
                // Compute duplicates.
                if(!uniques.add(name)) {
                    duplicates.add(name);
                }
            }
        }

        // Use duplicates to rename columns
        Map<String, DatasetExpression> result = new LinkedHashMap<>();
        for (Map.Entry<String, DatasetExpression> entry : datasets.entrySet()) {
            var name = entry.getKey();
            var dataset = entry.getValue();
            Map<String, String> fromTo = new LinkedHashMap<>();
            for (String columnName : dataset.getColumnNames()) {
                if (duplicates.contains(columnName)) {
                    fromTo.put(columnName, name + "#" + columnName);
                }
            }
            result.put(name, processingEngine.executeRename(dataset, fromTo));
        }

        return result;
    }

    private DatasetExpression leftJoin(VtlParser.JoinExprContext ctx) {
        var joinClauseContext = ctx.joinClause();
        var datasets = normalizeDatasets(joinClauseContext.joinClauseItem());

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

        return processingEngine.executeLeftJoin(renameDuplicates(commonIdentifiers, datasets), commonIdentifiers);
    }

    private DatasetExpression crossJoin(VtlParser.JoinExprContext ctx) {
        var joinClauseContext = ctx.joinClauseWithoutUsing();
        var datasets = normalizeDatasets(joinClauseContext.joinClauseItem());

        Map<String, DatasetExpression> renamedDatasets = renameDuplicates(List.of(), datasets);

        List<Component> identifiers = renamedDatasets.values().stream()
                .flatMap(dsExpr -> dsExpr.getDataStructure().values().stream())
                .filter(c -> c.isIdentifier())
                .collect(Collectors.toList());

        return processingEngine.executeCrossJoin(renamedDatasets, identifiers);
    }

    private DatasetExpression fullJoin(VtlParser.JoinExprContext ctx) {
        throw new UnsupportedOperationException("TODO: full_join");
    }

    private DatasetExpression innerJoin(VtlParser.JoinExprContext ctx) {
        var joinClauseContext = ctx.joinClause();
        var datasets = normalizeDatasets(joinClauseContext.joinClauseItem());

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

        return processingEngine.executeInnerJoin(renameDuplicates(commonIdentifiers, datasets), commonIdentifiers);
    }


}
