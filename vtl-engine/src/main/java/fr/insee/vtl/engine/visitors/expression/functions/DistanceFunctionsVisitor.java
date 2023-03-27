package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.utils.TypeChecking;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.InvalidTypeException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import org.apache.commons.text.similarity.LevenshteinDistance;

import java.util.Objects;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

/**
 * <code>DistanceFunctionsVisitor</code> is the base visitor for expressions involving distance functions.
 */
public class DistanceFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;

    /**
     * Constructor taking an expression visitor.
     *
     * @param expressionVisitor The visitor for the enclosing expression.
     */
    public DistanceFunctionsVisitor(ExpressionVisitor expressionVisitor) {
        exprVisitor = Objects.requireNonNull(expressionVisitor);
    }

    /**
     * Visits a 'Levenshtein distance' expression with two strings parameters.
     *
     * @param ctx The scripting context for the expression (left and right expressions should be the string parameters).
     * @return A <code>ResolvableExpression</code> resolving to a long integer representing the Levenshtein distance between the parameters.
     */
    @Override
    public ResolvableExpression visitLevenshteinAtom(VtlParser.LevenshteinAtomContext ctx) {
        var pos = fromContext(ctx);
        try {
            ResolvableExpression leftExpression = exprVisitor.visit(ctx.left).checkInstanceOf(String.class);
            ResolvableExpression rightExpression = exprVisitor.visit(ctx.right).checkInstanceOf(String.class);

            return ResolvableExpression.withType(Long.class).withPosition(pos)
                    .using(context -> {
                        String leftValue = (String) leftExpression.resolve(context);
                        String rightValue = (String) rightExpression.resolve(context);
                        if (TypeChecking.hasNullArgs(leftValue, rightValue)) return null;
                        return Long.valueOf(LevenshteinDistance.getDefaultInstance().apply(leftValue, rightValue));
                    });
        } catch (InvalidTypeException e) {
            throw new VtlRuntimeException(e);
        }
    }
}
