package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

/**
 * <code>ConstantVisitor</code> is the base visitor for constant expressions.
 */
public class ConstantVisitor extends VtlBaseVisitor<ResolvableExpression> {

    /**
     * Visits constants expressions.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the constant value with the expected type.
     */
    @Override
    public ResolvableExpression visitConstant(VtlParser.ConstantContext ctx) {
        if (ctx.INTEGER_CONSTANT() != null) {
            return ResolvableExpression.withType(Long.class, context -> Long.parseLong(ctx.getText()));
        }
        if (ctx.NUMBER_CONSTANT() != null) {
            return ResolvableExpression.withType(Double.class, context -> Double.parseDouble(ctx.getText()));
        }
        if (ctx.BOOLEAN_CONSTANT() != null) {
            return ResolvableExpression.withType(Boolean.class, context -> Boolean.parseBoolean(ctx.getText()));
        }
        if (ctx.STRING_CONSTANT() != null) {
            return ResolvableExpression.withType(String.class, context -> {
                String text = ctx.getText();
                return text.substring(1, text.length() - 1);
            });
        }
        if (ctx.NULL_CONSTANT() != null) {
            return ResolvableExpression.withType(Object.class, context -> null);
        }
        throw new UnsupportedOperationException("unknown constant type " + ctx);
    }
}
