package fr.insee.vtl.engine.visitors.component;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.util.Map;
import java.util.Objects;

/**
 * <code>ComponentExpressionVisitor</code> is the base visitor for expressions involving components.
 */
public class ComponentExpressionVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final Structured structuredExpression;

    /**
     * Constructor taking a data structure expression.
     *
     * @param structuredExpression An data structure expression to which the component expression is attached.
     */
    public ComponentExpressionVisitor(Structured structuredExpression) {
        this.structuredExpression = Objects.requireNonNull(structuredExpression);
    }

    /**
     * Visits comparison expressions between components.
     *
     * @param ctx The context of the comparison expression.
     * @return A <code>ResolvableExpression</code> that resolves to the comparison result.
     */
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

    /**
     * Visits identification expressions for components.
     *
     * @param ctx The context of the identification expression.
     * @return A <code>ResolvableExpression</code> that resolves to the component identifier.
     */
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
