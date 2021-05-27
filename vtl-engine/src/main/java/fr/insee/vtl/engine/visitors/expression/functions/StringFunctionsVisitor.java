package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.utils.TypeChecking;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.LongExpression;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.StringExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;
import java.util.regex.Pattern;

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
     * Constructor taking a scripting context.
     *
     * @param context The scripting context for the visitor.
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
        switch (ctx.op.getType()) {
            case VtlParser.TRIM:
                return handleTrim(expressionCtx);
            case VtlParser.LTRIM:
                return handleLTrim(expressionCtx);
            case VtlParser.RTRIM:
                return handleRTrim(expressionCtx);
            case VtlParser.UCASE:
                return handleUCase(expressionCtx);
            case VtlParser.LCASE:
                return handleLCase(expressionCtx);
            case VtlParser.LEN:
                return handleLen(expressionCtx);
            default:
                throw new UnsupportedOperationException("unknown operator " + ctx);
        }
    }

    private ResolvableExpression handleRTrim(VtlParser.ExprContext expressionCtx) {
        ResolvableExpression expression = assertString(exprVisitor.visit(expressionCtx), expressionCtx);
        return StringExpression.of(context -> {
            String value = (String) expression.resolve(context);
            if (value == null) return null;
            return RTRIM.matcher(value).replaceAll("");
        });
    }

    private ResolvableExpression handleUCase(VtlParser.ExprContext expressionCtx) {
        ResolvableExpression expression = assertString(exprVisitor.visit(expressionCtx), expressionCtx);
        return StringExpression.of(context -> {
            String value = (String) expression.resolve(context);
            if (value == null) return null;
            return value.toUpperCase();
        });
    }

    private ResolvableExpression handleLCase(VtlParser.ExprContext expressionCtx) {
        ResolvableExpression expression = assertString(exprVisitor.visit(expressionCtx), expressionCtx);
        return StringExpression.of(context -> {
            String value = (String) expression.resolve(context);
            if (value == null) return null;
            return value.toLowerCase();
        });
    }

    private ResolvableExpression handleLen(VtlParser.ExprContext expressionCtx) {
        ResolvableExpression expression = assertString(exprVisitor.visit(expressionCtx), expressionCtx);
        return LongExpression.of(context -> {
            String value = (String) expression.resolve(context);
            if (value == null) return null;
            return (long) value.length();
        });
    }

    private ResolvableExpression handleLTrim(VtlParser.ExprContext expressionCtx) {
        ResolvableExpression expression = assertString(exprVisitor.visit(expressionCtx), expressionCtx);
        return StringExpression.of(context -> {
            String value = (String) expression.resolve(context);
            if (value == null) return null;
            return LTRIM.matcher(value).replaceAll("");
        });
    }

    private ResolvableExpression handleTrim(VtlParser.ExprContext expressionCtx) {
        ResolvableExpression expression = assertString(exprVisitor.visit(expressionCtx), expressionCtx);
        return StringExpression.of(context -> {
            String value = (String) expression.resolve(context);
            if (value == null) return null;
            return value.trim();
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
            String args = String.valueOf((ctx.children.size() - 4) / 2);
            // TODO: Define subclass of VtlScriptException. Ideally there should be not distinction between a core
            //  function and a dynamically defined function. This could be a subclass of InvalidType. Anyways need
            //  to do some research.
            throw new UnsupportedOperationException("too many args (" + args + ") for: " + ctx.getText());
        }
        // TODO: grammar issue: endParameter should be named lengthParameter
        ResolvableExpression expression = exprVisitor.visit(ctx.expr());
        var startExpression = ctx.startParameter == null ? LongExpression.of(0L)
                : assertLong(exprVisitor.visit(ctx.startParameter), ctx.startParameter);
        var lengthExpression = ctx.endParameter == null ? null
                : assertLong(exprVisitor.visit(ctx.endParameter), ctx.endParameter);

        return ResolvableExpression.withType(String.class, context -> {
            String value = (String) expression.resolve(context);
            if (value == null) return null;
            Long startValue = (Long) startExpression.resolve(context);
            if (startValue == null) return null;
            if (startValue > value.length()) return "";
            var start = startValue.equals(0L) ? 0 : startValue.intValue()  - 1;
            if (lengthExpression == null) return value.substring(start);
            Long lengthValue = (Long) lengthExpression.resolve(context);
            if (lengthValue == null) return null;
            var end = start + lengthValue.intValue();
            if (end > value.length()) return value.substring(start);;
            return value.substring(start, end);
        });
    }

    /**
     * Visits expressions corresponding to the replace function on a string operand.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the result of the replace function on the operand.
     */
    @Override
    public ResolvableExpression visitReplaceAtom(VtlParser.ReplaceAtomContext ctx) {
        ResolvableExpression expression = assertString(exprVisitor.visit(ctx.expr(0)), ctx.expr(0));
        ResolvableExpression inputPattern = assertString(exprVisitor.visit(ctx.param), ctx.param);

        var outputPattern = ctx.optionalExpr() == null ? StringExpression.of("")
                : assertString(exprVisitor.visit(ctx.optionalExpr()), ctx.optionalExpr());

        return ResolvableExpression.withType(String.class, context -> {
            String value = (String) expression.resolve(context);
            String inputPatternValue = (String) inputPattern.resolve(context);
            String outputPatternValue = (String) outputPattern.resolve(context);
            if (TypeChecking.hasNullArgs(value, inputPatternValue, outputPatternValue)) return null;
            return value.replaceAll(inputPatternValue, outputPatternValue);
        });
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

        var occurence = ctx.occurrenceParameter == null ? LongExpression.of(1L)
                : assertLong(exprVisitor.visit(ctx.occurrenceParameter), ctx.occurrenceParameter);

        return ResolvableExpression.withType(Long.class, context -> {
            String value = (String) expression.resolve(context);
            String patternValue = (String) pattern.resolve(context);
            Long startValue = (Long) start.resolve(context);
            Long occurenceValue = (Long) occurence.resolve(context);
            if (TypeChecking.hasNullArgs(value, patternValue, startValue, occurenceValue)) return null;
            return StringUtils.ordinalIndexOf(value.substring(startValue.intValue()), patternValue, occurenceValue.intValue()) + 1L;
        });
    }

}
