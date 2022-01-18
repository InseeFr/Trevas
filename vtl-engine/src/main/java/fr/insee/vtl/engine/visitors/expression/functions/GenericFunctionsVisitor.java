package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.*;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.time.LocalDate;
import java.util.Objects;

/**
 * <code>GenericFunctionsVisitor</code> is the base visitor for cast expressions.
 */
public class GenericFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;

    /**
     * Constructor taking an expression visitor.
     *
     * @param expressionVisitor The visitor for the enclosing expression.
     */
    public GenericFunctionsVisitor(ExpressionVisitor expressionVisitor) {
        exprVisitor = Objects.requireNonNull(expressionVisitor);
    }

    /**
     * Method to map basic scalar types and classes.
     *
     * @param basicScalarType Basic scalar type.
     * @param basicScalarText Basic scalar text.
     */
    private static Class<?> getOutputClass(Integer basicScalarType, String basicScalarText) {
        switch (basicScalarType) {
            case VtlParser.STRING:
                return String.class;
            case VtlParser.INTEGER:
                return Long.class;
            case VtlParser.NUMBER:
                return Double.class;
            case VtlParser.BOOLEAN:
                return Boolean.class;
            case VtlParser.DATE:
                return LocalDate.class;
            default:
                throw new UnsupportedOperationException("basic scalar type " + basicScalarText + " unsupported");
        }
    }

    /**
     * Visits expressions with cast operators.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the result of the cast operation.
     */
    @Override
    public ResolvableExpression visitCastExprDataset(VtlParser.CastExprDatasetContext ctx) {
        ResolvableExpression expression = exprVisitor.visit(ctx.expr());
        TerminalNode maskNode = ctx.STRING_CONSTANT();
        // STRING_CONSTANT().getText return null or a string wrapped by quotes
        String mask = maskNode == null ? null :
                maskNode.getText()
                        .replaceAll("\"", "")
                        .replace("YYYY", "yyyy")
                        .replace("DD", "dd");
        Token symbol = ((TerminalNode) ctx.basicScalarType().getChild(0)).getSymbol();
        Integer basicScalarType = symbol.getType();
        String basicScalarText = symbol.getText();

        Class<?> outputClass = getOutputClass(basicScalarType, basicScalarText);

        if (Object.class.equals(expression.getType())) {
            return ResolvableExpression.ofType(outputClass, null);
        }
        if (String.class.equals(expression.getType())) {
            return StringExpression.castTo(expression, outputClass, mask);
        }
        if (Boolean.class.equals(expression.getType())) {
            return BooleanExpression.castTo(expression, outputClass);
        }
        if (Long.class.equals(expression.getType())) {
            return LongExpression.castTo(expression, outputClass);
        }
        if (Double.class.equals(expression.getType())) {
            return DoubleExpression.castTo(expression, outputClass);
        }
        if (LocalDate.class.equals(expression.getType())) {
            return DateExpression.castTo(expression, outputClass, mask);
        }
        throw new UnsupportedOperationException("cast unsupported on expression of type: " + expression.getType());
    }
}
