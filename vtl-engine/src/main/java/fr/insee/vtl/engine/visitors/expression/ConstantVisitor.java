package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.model.BooleanExpression;
import fr.insee.vtl.model.DoubleExpression;
import fr.insee.vtl.model.LongExpression;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.StringExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

/**
 * <code>ConstantVisitor</code> is the base visitor for constant expressions.
 */
public class ConstantVisitor extends VtlBaseVisitor<ResolvableExpression> {

    /**
     * Visits constant expressions.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the constant value with the expected type.
     */
    @Override
    public ResolvableExpression visitConstant(VtlParser.ConstantContext ctx) {
        if (ctx.INTEGER_CONSTANT() != null) {
            return LongExpression.of(Long.parseLong(ctx.getText()));
        }
        if (ctx.NUMBER_CONSTANT() != null) {
            return DoubleExpression.of(Double.parseDouble(ctx.getText()));
        }
        if (ctx.BOOLEAN_CONSTANT() != null) {
            return BooleanExpression.of(Boolean.parseBoolean(ctx.getText()));
        }
        if (ctx.STRING_CONSTANT() != null) {
            var text = ctx.getText();
            return StringExpression.of(text.substring(1, text.length() - 1));
        }
        if (ctx.NULL_CONSTANT() != null) {
            return ResolvableExpression.withType(Object.class, context -> null);
        }
        throw new UnsupportedOperationException("unknown constant type " + ctx);
    }
}
