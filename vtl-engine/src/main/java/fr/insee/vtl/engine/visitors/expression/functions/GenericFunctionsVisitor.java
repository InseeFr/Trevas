package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.UnsupportedTypeException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.*;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

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
     * Visits expressions with cast operators.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the result of the cast operation.
     */
    @Override
    public ResolvableExpression visitCastExprDataset(VtlParser.CastExprDatasetContext ctx) {
        ResolvableExpression expression = exprVisitor.visit(ctx.expr());
        Integer basicScalarType = ((TerminalNode) ctx.basicScalarType().getChild(0)).getSymbol().getType();

        // Special case with nulls.
        if (expression.getType().equals(Object.class)) {
            if (basicScalarType.equals(VtlParser.INTEGER))
                return LongExpression.of((Long) null);
            if (basicScalarType.equals(VtlParser.NUMBER))
                return DoubleExpression.of((Double) null);
            if (basicScalarType.equals(VtlParser.STRING))
                return StringExpression.of((String) null);
            if (basicScalarType.equals(VtlParser.BOOLEAN))
                return BooleanExpression.of((Boolean) null);
            throw new UnsupportedOperationException("Cast second argument has to be a basic scalar type but is: ");
        }

        return LongExpression.of((Long) null);
    }
}
