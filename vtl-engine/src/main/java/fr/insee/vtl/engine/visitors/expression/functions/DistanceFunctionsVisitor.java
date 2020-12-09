package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.utils.TypeChecking;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.StringExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import org.apache.commons.text.similarity.LevenshteinDistance;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static fr.insee.vtl.engine.utils.TypeChecking.assertString;

/**
 * <code>DistanceFunctionsVisitor</code> is the base visitor for expressions involving distance functions.
 */
public class DistanceFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;

    /**
     * Constructor taking a scripting context.
     *
     * @param context The scripting context for the visitor.
     */
    public DistanceFunctionsVisitor(ExpressionVisitor expressionVisitor) {
        exprVisitor = Objects.requireNonNull(expressionVisitor);
    }

    /**
     * Visits a 'levenshtein' expression with two strings parameters.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to a long (<code>10</code> representing Levenshtein distance between two string params).
     */
    @Override
    public ResolvableExpression visitLevenshteinAtom(VtlParser.LevenshteinAtomContext ctx) {
        ResolvableExpression leftExpression = assertString(exprVisitor.visit(ctx.left), ctx.left);
        ResolvableExpression rightExpression = assertString(exprVisitor.visit(ctx.right), ctx.right);

        return ResolvableExpression.withType(Long.class, context -> {
            String leftValue = (String) leftExpression.resolve(context);
            String rightValue = (String) rightExpression.resolve(context);
            if(TypeChecking.hasNullArgs(leftValue, rightValue)) return null;
            return Long.valueOf(LevenshteinDistance.getDefaultInstance().apply(leftValue, rightValue));
        });
    }
}
