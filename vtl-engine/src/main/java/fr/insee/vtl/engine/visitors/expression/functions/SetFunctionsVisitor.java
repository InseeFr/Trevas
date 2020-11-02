package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

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
        ResolvableExpression first = assertTypeExpression(expressionVisitor.visit(ctx.left), Dataset.class, ctx.left);
        datasets.add((DatasetExpression) first);
        for (VtlParser.ExprContext expr : ctx.expr()) {
            ResolvableExpression rest = assertTypeExpression(expressionVisitor.visit(expr), Dataset.class, expr);
            datasets.add((DatasetExpression) rest);
        }

        // TODO: Check that the structure is the same.

        return new DatasetExpression() {
            @Override
            public Dataset resolve(Map<String, Object> context) {
                Dataset ds1 = datasets.get(0).resolve(context);
                Dataset ds2 = datasets.get(1).resolve(context);
                List<DataPoint> data = Stream.concat(
                        ds1.getDataPoints().stream(),
                        ds2.getDataPoints().stream()
                ).distinct().collect(Collectors.toList());
                return new InMemoryDataset(data, getDataStructure());
            }

            @Override
            public DataStructure getDataStructure() {
                return ((DatasetExpression) first).getDataStructure();
            }
        };

    }
}
