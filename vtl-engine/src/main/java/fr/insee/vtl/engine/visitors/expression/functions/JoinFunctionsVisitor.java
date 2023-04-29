package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;
import static fr.insee.vtl.engine.utils.TypeChecking.assertTypeExpression;
import static fr.insee.vtl.model.Dataset.Component;
import static fr.insee.vtl.model.Dataset.Role;

/**
 * <code>JoinFunctionsVisitor</code> is the visitor for expressions involving join functions (left, inner, full, etc.).
 */
public class JoinFunctionsVisitor extends VtlBaseVisitor<DatasetExpression> {

    private final ExpressionVisitor expressionVisitor;
    private final ProcessingEngine processingEngine;

    /**
     * Constructor taking an expression visitor and a processing engine.
     *
     * @param expressionVisitor A visitor for the expression corresponding to the join function.
     * @param processingEngine  The processing engine.
     */
    public JoinFunctionsVisitor(ExpressionVisitor expressionVisitor, ProcessingEngine processingEngine) {
        this.expressionVisitor = Objects.requireNonNull(expressionVisitor);
        this.processingEngine = Objects.requireNonNull(processingEngine);
    }

    public static Optional<List<Component>> checkSameIdentifiers(Collection<DatasetExpression> datasetExpressions) {
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
        List<String> measures = new ArrayList<>();
        for (VtlParser.JoinClauseItemContext joinClauseItem : joinClauseItems) {
            var datasetExpressionContext = joinClauseItem.expr();
            var alias = joinClauseItem.alias() != null ? joinClauseItem.alias().IDENTIFIER().getText() : null;
            if (alias == null && !(datasetExpressionContext instanceof VtlParser.VarIdExprContext)) {
                throw new VtlRuntimeException(
                        new InvalidArgumentException("cannot use expression without alias in join clause", fromContext(datasetExpressionContext))
                );
            }

            var datasetExpression = (DatasetExpression) assertTypeExpression(
                    expressionVisitor.visit(datasetExpressionContext),
                    Dataset.class, datasetExpressionContext);
            List<String> dsMeasures = datasetExpression.getDataStructure().values().stream()
                    .filter(Component::isMeasure)
                    .map(Component::getName)
                    .collect(Collectors.toList());
            if (alias == null) {
                dsMeasures.forEach(m -> {
                    if (measures.contains(m)) {
                        throw new VtlRuntimeException(
                                new InvalidArgumentException("It is not allowed that two or more Components in the virtual Data Set have the same name", fromContext(datasetExpressionContext))
                        );
                    }
                });
                datasets.put(datasetExpressionContext.getText(), datasetExpression);
            } else {
                datasets.put(alias, datasetExpression);
            }
            measures.addAll(dsMeasures);
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
                if (!uniques.add(name)) {
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

    private DatasetExpression removeComponentAlias(DatasetExpression dataset) {
        Set<String> toKeep = dataset.getDataStructure().values().stream()
                .map(Component::getName)
                .map(n -> n.substring(n.lastIndexOf("#") + 1))
                .collect(Collectors.toSet());
        List<String> componentNamesWithAlias = dataset.getDataStructure().values().stream()
                .map(Component::getName)
                .filter(n -> n.contains("#")).collect(Collectors.toList());
        Map<String, String> toFrom = new HashMap<>();
        componentNamesWithAlias.forEach(n -> {
            String extraction = n.substring(n.lastIndexOf("#") + 1);
            toFrom.put(extraction, n);
        });
        Map<String, String> fromTo = toFrom.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        var renamed = processingEngine.executeRename(dataset, fromTo);
        return processingEngine.executeProject(renamed, new ArrayList<>(toKeep));
    }

    private DatasetExpression leftJoin(VtlParser.JoinExprContext ctx) {
        var joinClauseContext = ctx.joinClause();
        var datasets = normalizeDatasets(joinClauseContext.joinClauseItem());

        // Left join require that all the datasets have one or more common identifiers.
        var commonIdentifiers = checkSameIdentifiers(datasets.values())
                .orElseThrow(() -> new VtlRuntimeException(
                        new InvalidArgumentException("datasets must have common identifiers", fromContext(joinClauseContext))
                ));

        // Remove the identifiers that are not part of the "using" list
        if (joinClauseContext.USING() != null) {
            var identifierNames = commonIdentifiers.stream()
                    .map(Component::getName)
                    .collect(Collectors.toList());
            var usingNames = new ArrayList<String>();
            for (VtlParser.ComponentIDContext usingContext : joinClauseContext.componentID()) {
                var name = usingContext.getText();
                if (!identifierNames.contains(name)) {
                    throw new VtlRuntimeException(
                            new InvalidArgumentException("not in the set of common identifiers", fromContext(usingContext))
                    );
                }
                usingNames.add(name);
            }
            commonIdentifiers.removeIf(component -> !usingNames.contains(component.getName()));
        }

        DatasetExpression res = processingEngine.executeLeftJoin(renameDuplicates(commonIdentifiers, datasets), commonIdentifiers);
        return removeComponentAlias(res);
    }

    private DatasetExpression crossJoin(VtlParser.JoinExprContext ctx) {
        var joinClauseContext = ctx.joinClauseWithoutUsing();
        var datasets = normalizeDatasets(joinClauseContext.joinClauseItem());

        Map<String, DatasetExpression> renamedDatasets = renameDuplicates(List.of(), datasets);

        List<Component> identifiers = renamedDatasets.values().stream()
                .flatMap(dsExpr -> dsExpr.getDataStructure().values().stream())
                .filter(Component::isIdentifier)
                .collect(Collectors.toList());

        return processingEngine.executeCrossJoin(renamedDatasets, identifiers);
    }

    private DatasetExpression fullJoin(VtlParser.JoinExprContext ctx) {
        var joinClauseContext = ctx.joinClauseWithoutUsing();
        var datasets = normalizeDatasets(joinClauseContext.joinClauseItem());

        // Full join require that all the datasets have one or more common identifiers.
        var commonIdentifiers = checkSameIdentifiers(datasets.values())
                .orElseThrow(() -> new VtlRuntimeException(
                        new InvalidArgumentException("datasets must have common identifiers", fromContext(joinClauseContext))
                ));

        DatasetExpression res = processingEngine.executeFullJoin(renameDuplicates(commonIdentifiers, datasets), commonIdentifiers);
        return removeComponentAlias(res);
    }

    private DatasetExpression innerJoin(VtlParser.JoinExprContext ctx) {
        var joinClauseContext = ctx.joinClause();
        var datasets = normalizeDatasets(joinClauseContext.joinClauseItem());

        // Left join require that all the datasets have the same identifiers.
        var commonIdentifiers = checkSameIdentifiers(datasets.values())
                .orElseThrow(() -> new VtlRuntimeException(
                        new InvalidArgumentException("datasets must have common identifiers", fromContext(joinClauseContext))
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
                            new InvalidArgumentException("not in the set of common identifiers", fromContext(usingContext))
                    );
                }
                usingNames.add(name);
            }
            commonIdentifiers.removeIf(component -> !usingNames.contains(component.getName()));
        }
        DatasetExpression res = processingEngine.executeInnerJoin(renameDuplicates(commonIdentifiers, datasets), commonIdentifiers);
        return removeComponentAlias(res);
    }
}
