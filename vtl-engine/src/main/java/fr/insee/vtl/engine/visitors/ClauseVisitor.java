package fr.insee.vtl.engine.visitors;

import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class ClauseVisitor extends VtlBaseVisitor<DatasetExpression> {

    private final DatasetExpression datasetExpression;
    private final ExpressionVisitor componentExpressionVisitor;

    private final ProcessingEngine processingEngine;

    public ClauseVisitor(DatasetExpression datasetExpression, ProcessingEngine processingEngine) {
        this.datasetExpression = Objects.requireNonNull(datasetExpression);
        // Here we "switch" to the dataset context.
        Map<String, Object> componentMap = datasetExpression.getDataStructure().stream()
                .collect(Collectors.toMap(Dataset.Component::getName, component -> component));
        this.componentExpressionVisitor = new ExpressionVisitor(componentMap, processingEngine);
        this.processingEngine = Objects.requireNonNull(processingEngine);
    }

    private static String getName(VtlParser.ComponentIDContext context) {
        // TODO: Should be an expression so we can handle membership better and use the exceptions
        //  for undefined var etc.
        return context.getText();
    }

    @Override
    public DatasetExpression visitKeepOrDropClause(VtlParser.KeepOrDropClauseContext ctx) {
        // Normalize to keep operation.
        var keep = ctx.op.getType() == VtlParser.KEEP;
        var names = ctx.componentID().stream().map(ClauseVisitor::getName)
                .collect(Collectors.toSet());
        List<String> columnNames = datasetExpression.getDataStructure().stream().map(Dataset.Component::getName)
                .filter(name -> keep == names.contains(name))
                .collect(Collectors.toList());

        return processingEngine.executeProject(datasetExpression, columnNames);
    }

    @Override
    public DatasetExpression visitCalcClause(VtlParser.CalcClauseContext ctx) {

        var expressions = new LinkedHashMap<String, ResolvableExpression>();
        var roles = new LinkedHashMap<String, Dataset.Role>();
        for (VtlParser.CalcClauseItemContext calcCtx : ctx.calcClauseItem()) {
            var columnName = calcCtx.componentID().getText();
            var columnRole = calcCtx.componentRole() == null ? Dataset.Role.MEASURE
                    : Dataset.Role.valueOf(calcCtx.componentRole().getText().toUpperCase());
            ResolvableExpression calc = componentExpressionVisitor.visit(calcCtx);
            expressions.put(columnName, calc);
            roles.put(columnName, columnRole);
        }

        return processingEngine.executeCalc(datasetExpression, expressions, roles);
    }

    @Override
    public DatasetExpression visitFilterClause(VtlParser.FilterClauseContext ctx) {
        ResolvableExpression filter = componentExpressionVisitor.visit(ctx.expr());
        return processingEngine.executeFilter(datasetExpression, filter);
    }

    @Override
    public DatasetExpression visitRenameClause(VtlParser.RenameClauseContext ctx) {
        Map<String, String> fromTo = new LinkedHashMap<>();
        for (VtlParser.RenameClauseItemContext renameCtx : ctx.renameClauseItem()) {
            fromTo.put(getName(renameCtx.fromName), getName(renameCtx.toName));
        }
        return processingEngine.executeRename(datasetExpression, fromTo);
    }
}
