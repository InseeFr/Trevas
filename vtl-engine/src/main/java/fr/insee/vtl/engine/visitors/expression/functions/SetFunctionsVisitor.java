package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.exceptions.VtlScriptException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.*;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import org.antlr.v4.runtime.RuleContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static fr.insee.vtl.engine.utils.TypeChecking.assertTypeExpression;

public class SetFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor expressionVisitor;

    private final ProcessingEngine processingEngine;

    public SetFunctionsVisitor(ExpressionVisitor expressionVisitor, ProcessingEngine processingEngine) {
        this.expressionVisitor = Objects.requireNonNull(expressionVisitor);
        this.processingEngine = Objects.requireNonNull(processingEngine);
    }

    @Override
    public ResolvableExpression visitUnionAtom(VtlParser.UnionAtomContext ctx) {

        List<DatasetExpression> datasets = new ArrayList<>();
        Structured.DataStructure structure = null;
        for (VtlParser.ExprContext expr : ctx.expr()) {
            DatasetExpression rest = (DatasetExpression) assertTypeExpression(expressionVisitor.visit(expr),
                    Dataset.class, expr);
            datasets.add(rest);

            // Check that all the structures are the same.
            if (structure == null) {
                structure = rest.getDataStructure();
            } else if (!structure.equals(rest.getDataStructure())) {
                // TODO: Create exception
                throw new VtlRuntimeException(new VtlScriptException(
                        String.format(
                                "dataset structure of %s is incompatible with %s",
                                expr.getText(),
                                ctx.expr().stream().map(RuleContext::getText)
                                        .collect(Collectors.joining(", "))
                        ),
                        ctx
                ));
            }

        }

        return processingEngine.executeUnion(datasets);
    }
}
