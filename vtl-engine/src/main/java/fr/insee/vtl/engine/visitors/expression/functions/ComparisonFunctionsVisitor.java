package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.exceptions.ConflictingTypesException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.utils.TypeChecking;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static fr.insee.vtl.engine.utils.NumberConvertors.asBigDecimal;
import static fr.insee.vtl.engine.utils.TypeChecking.*;

/**
 * <code>ComparisonFunctionsVisitor</code> is the base visitor for expressions involving comparison functions.
 */
public class ComparisonFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;

    /**
     * Constructor taking an expression visitor.
     *
     * @param expressionVisitor The visitor for the enclosing expression.
     */
    public ComparisonFunctionsVisitor(ExpressionVisitor expressionVisitor) {
        exprVisitor = Objects.requireNonNull(expressionVisitor);
    }

    /**
     * Visits a 'between' expression with scalar operand and delimiters.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to a boolean (<code>true</code> if the operand is between the delimiters).
     */
    @Override
    public ResolvableExpression visitBetweenAtom(VtlParser.BetweenAtomContext ctx) {
        ResolvableExpression operandExpression = exprVisitor.visit(ctx.op);
        ResolvableExpression fromExpression = exprVisitor.visit(ctx.from_);
        ResolvableExpression toExpression = exprVisitor.visit(ctx.to_);

        // TODO: handle other types (dates?)

        assertTypeExpressionAcceptDoubleLong(operandExpression, fromExpression.getType(), ctx.op);
        assertTypeExpressionAcceptDoubleLong(operandExpression, toExpression.getType(), ctx.op);

        return ResolvableExpression.withType(Boolean.class, context -> {
            BigDecimal operandValue = asBigDecimal(operandExpression, operandExpression.resolve(context));
            BigDecimal fromValue = asBigDecimal(fromExpression, fromExpression.resolve(context));
            BigDecimal toValue = asBigDecimal(toExpression, toExpression.resolve(context));
            if (TypeChecking.hasNullArgs(operandValue, fromValue, toValue)) return null;
            return operandValue.compareTo(fromValue) >= 0 && operandValue.compareTo(toValue) <= 0;
        });
    }

    /**
     * Visits a pattern matching expression with string operand and regular expression.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to a boolean (<code>true</code> if the operand matches the pattern).
     */
    @Override
    public ResolvableExpression visitCharsetMatchAtom(VtlParser.CharsetMatchAtomContext ctx) {
        ResolvableExpression operandExpression = assertString(exprVisitor.visit(ctx.op), ctx.op);
        ResolvableExpression patternExpression = assertString(exprVisitor.visit(ctx.pattern), ctx.pattern);

        return ResolvableExpression.withType(Boolean.class, context -> {
            String operandValue = (String) operandExpression.resolve(context);
            String patternValue = (String) patternExpression.resolve(context);
            if (TypeChecking.hasNullArgs(operandValue, patternValue)) return null;
            Pattern pattern = Pattern.compile(patternValue);
            Matcher matcher = pattern.matcher(operandValue);
            return matcher.matches();
        });
    }

    /**
     * Visits a null testing expression with scalar operand.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to a boolean (<code>true</code> if the operand is null).
     */
    @Override
    public ResolvableExpression visitIsNullAtom(VtlParser.IsNullAtomContext ctx) {
        ResolvableExpression operandExpression = exprVisitor.visit(ctx.expr());
        return ResolvableExpression.withType(Boolean.class, context ->
                operandExpression.resolve(context) == null
        );
    }
}
