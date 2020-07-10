package fr.insee.vtl.engine.visitors.component;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.util.Map;
import java.util.Objects;

public class ComponentExpressionVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final Structured structuredExpression;

    public ComponentExpressionVisitor(Structured structuredExpression) {
        this.structuredExpression = Objects.requireNonNull(structuredExpression);
    }

    @Override
    public ResolvableExpression visitComparisonExprComp(VtlParser.ComparisonExprCompContext ctx) {

        ResolvableExpression leftExpression = visit(ctx.left);
        ResolvableExpression rightExpression = visit(ctx.right);

        if (ctx.comparisonOperand().EQ() != null) {
            return new ResolvableExpression() {

                @Override
                public Object resolve(Map<String, Object> context) {
                    Object leftValue = leftExpression.resolve(context);
                    Object rightValue = rightExpression.resolve(context);
                    return leftValue.equals(rightValue);
                }

                @Override
                public Class<?> getType() {
                    return Boolean.class;
                }
            };
        } else {
            throw new UnsupportedOperationException("implement the other comparison");
        }
    }

    @Override
    public ResolvableExpression visitCompId(VtlParser.CompIdContext ctx) {
        String columnName = ctx.componentID().getText();
        return new ResolvableExpression() {

            @Override
            public Object resolve(Map<String, Object> context) {
                return context.get(columnName);
            }

            @Override
            public Class<?> getType() {
                return structuredExpression.getDataStructure().stream()
                        .filter(structure -> columnName.equals(structure.getName()))
                        .map(Dataset.Structure::getType).findFirst().orElseThrow();
            }
        };
    }
}
