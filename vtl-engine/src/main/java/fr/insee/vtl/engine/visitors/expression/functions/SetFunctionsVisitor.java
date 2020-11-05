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
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static fr.insee.vtl.engine.utils.TypeChecking.assertTypeExpression;

public class SetFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor expressionVisitor;

    public SetFunctionsVisitor(ExpressionVisitor expressionVisitor) {
        this.expressionVisitor = Objects.requireNonNull(expressionVisitor);
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

        return new DatasetExpression() {
            @Override
            public Dataset resolve(Map<String, Object> context) {
                Stream<DataPoint> stream = Stream.empty();
                for (DatasetExpression datasetExpression : datasets) {
                    var dataset = datasetExpression.resolve(context);
                    stream = Stream.concat(stream, dataset.getDataPoints().stream());
                }
                List<DataPoint> data = stream.distinct().collect(Collectors.toList());
                return new InMemoryDataset(data, getDataStructure());
            }

            @Override
            public DataStructure getDataStructure() {
                return (datasets.get(0)).getDataStructure();
            }
        };

    }
}
