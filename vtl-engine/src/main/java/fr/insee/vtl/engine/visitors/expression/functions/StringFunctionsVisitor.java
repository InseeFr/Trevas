package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.utils.TypeChecking;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.InvalidTypeException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Pattern;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;
import static fr.insee.vtl.engine.utils.TypeChecking.assertLong;
import static fr.insee.vtl.engine.utils.TypeChecking.assertString;

/**
 * <code>ComparisonFunctionsVisitor</code> is the base visitor for expressions involving string functions.
 */
public class StringFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final Pattern LTRIM = Pattern.compile("^\\s+");
    private final Pattern RTRIM = Pattern.compile("\\s+$");

    private final ExpressionVisitor exprVisitor;

    /**
     * Constructor taking an expression visitor.
     *
     * @param expressionVisitor The visitor for the enclosing expression.
     */
    public StringFunctionsVisitor(ExpressionVisitor expressionVisitor) {
        exprVisitor = Objects.requireNonNull(expressionVisitor);
    }

    /**
     * Visits expressions corresponding to unary string functions.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the result of the string function on the operand.
     */
    @Override
    public ResolvableExpression visitUnaryStringFunction(VtlParser.UnaryStringFunctionContext ctx) {
        VtlParser.ExprContext expressionCtx = ctx.expr();
        ResolvableExpression expression;
        try {
            expression = exprVisitor.visit(expressionCtx).checkInstanceOf(String.class);
        } catch (InvalidTypeException e) {
            throw new VtlRuntimeException(e);
        }

        var pos = fromContext(ctx);

        Function<String, String> handle = null;

        switch (ctx.op.getType()) {
            case VtlParser.TRIM:
                handle = String::trim;
                break;
            case VtlParser.LTRIM:
                handle = input -> LTRIM.matcher(input).replaceAll("");
                break;
            case VtlParser.RTRIM:
                handle = input -> RTRIM.matcher(input).replaceAll("");
                break;
            case VtlParser.UCASE:
                handle = String::toUpperCase;
                break;
            case VtlParser.LCASE:
                handle = String::toLowerCase;
                break;
            case VtlParser.LEN:
                return ResolvableExpression.withType(Long.class).withPosition(pos)
                        .using(context -> {
                            String value = (String) expression.resolve(context);
                            if (value == null) return null;
                            return (long) value.length();
                        });
        }

        if (handle == null) throw new UnsupportedOperationException("unknown operator " + ctx);

        Function<String, String> finalHandle = handle;
        return ResolvableExpression.withType(String.class).withPosition(pos)
                .using(context -> {
                    String value = (String) expression.resolve(context);
                    if (value == null) return null;
                    return finalHandle.apply(value);
                });
    }

    /**
     * Visits expressions corresponding to the substring function on a string operand.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the result of the substring function on the operand.
     */
    @Override
    public ResolvableExpression visitSubstrAtom(VtlParser.SubstrAtomContext ctx) {
        if (ctx.children.size() > 8) {
            // Compute the amount of parameters (excluding the tokens etc.)
            String args = String.valueOf((ctx.children.size() - 4) / 2);
            throw new VtlRuntimeException(new InvalidArgumentException(
                    "too many args (" + args + ")", fromContext(ctx)));
        }
        // TODO: grammar issue: endParameter should be named lengthParameter
        ResolvableExpression expression = exprVisitor.visit(ctx.expr());
        var pos = fromContext(ctx);
        ResolvableExpression startExpression;
        ResolvableExpression lengthExpression;
        try {
            startExpression = ctx.startParameter == null ?
                    ResolvableExpression.withType(Long.class).withPosition(pos).using(c -> 0L)
                    : exprVisitor.visit(ctx.startParameter).checkInstanceOf(Long.class);
            lengthExpression = ctx.endParameter == null ? null
                    : exprVisitor.visit(ctx.endParameter).checkInstanceOf(Long.class);
        } catch (InvalidTypeException e) {
            throw new VtlRuntimeException(e);
        }

        return ResolvableExpression.withType(String.class).withPosition(pos).using(
                context -> {
                    String value = (String) expression.resolve(context);
                    if (value == null) return null;
                    Long startValue = (Long) startExpression.resolve(context);
                    if (startValue == null) return null;
                    if (startValue > value.length()) return "";
                    var start = startValue.equals(0L) ? 0 : startValue.intValue() - 1;
                    if (lengthExpression == null) return value.substring(start);
                    Long lengthValue = (Long) lengthExpression.resolve(context);
                    if (lengthValue == null) return null;
                    var end = start + lengthValue.intValue();
                    if (end > value.length()) return value.substring(start);
                    return value.substring(start, end);
                }
        );
    }

    /**
     * Visits expressions corresponding to the replace function on a string operand.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the result of the replace function on the operand.
     */
    @Override
    public ResolvableExpression visitReplaceAtom(VtlParser.ReplaceAtomContext ctx) {
        var pos = fromContext(ctx);
        try {
            ResolvableExpression expression = exprVisitor.visit(ctx.expr(0)).checkInstanceOf(String.class);
            ResolvableExpression inputPattern = exprVisitor.visit(ctx.param).checkInstanceOf(String.class);
            ResolvableExpression outputPattern = ctx.optionalExpr() == null ?
                    ResolvableExpression.withType(String.class).withPosition(pos).using(c -> "")
                    : exprVisitor.visit(ctx.optionalExpr()).checkInstanceOf(String.class);

            return ResolvableExpression.withType(String.class).withPosition(pos).using(
                    context -> {
                        String value = (String) expression.resolve(context);
                        String inputPatternValue = (String) inputPattern.resolve(context);
                        String outputPatternValue = (String) outputPattern.resolve(context);
                        if (TypeChecking.hasNullArgs(value, inputPatternValue, outputPatternValue)) return null;
                        return value.replaceAll(inputPatternValue, outputPatternValue);
                    }
            );
        } catch (InvalidTypeException e) {
            throw new VtlRuntimeException(e);
        }
    }

    /**
     * Visits expressions corresponding to the pattern location function on a string operand.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the result of the pattern location function on the operand.
     */
    @Override
    public ResolvableExpression visitInstrAtom(VtlParser.InstrAtomContext ctx) {
        ResolvableExpression expression = assertString(exprVisitor.visit(ctx.expr(0)), ctx.expr(0));
        ResolvableExpression pattern = assertString(exprVisitor.visit(ctx.pattern), ctx.pattern);

        var start = ctx.startParameter == null ? LongExpression.of(0L)
                : assertLong(exprVisitor.visit(ctx.startParameter), ctx.startParameter);

        var occurrence = ctx.occurrenceParameter == null ? LongExpression.of(1L)
                : assertLong(exprVisitor.visit(ctx.occurrenceParameter), ctx.occurrenceParameter);

        var pos = fromContext(ctx);

        return ResolvableExpression.withType(Long.class).withPosition(pos).using(
                context -> {
                    String value = (String) expression.resolve(context);
                    String patternValue = (String) pattern.resolve(context);
                    Long startValue = (Long) start.resolve(context);
                    Long occurrenceValue = (Long) occurrence.resolve(context);
                    if (TypeChecking.hasNullArgs(value, patternValue, startValue, occurrenceValue)) return null;
                    return StringUtils.ordinalIndexOf(value.substring(startValue.intValue()), patternValue, occurrenceValue.intValue()) + 1L;
                }
        );
    }
}
