package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.utils.TypeChecking;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.InvalidTypeException;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Pattern;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

/**
 * <code>ComparisonFunctionsVisitor</code> is the base visitor for expressions involving string functions.
 */
public class StringFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

    static final Pattern LTRIM = Pattern.compile("^\\s+");
    static final Pattern RTRIM = Pattern.compile("\\s+$");

    private final ExpressionVisitor exprVisitor;
    private final GenericFunctionsVisitor genericFunctionsVisitor;

    /**
     * Constructor taking an expression visitor.
     *
     * @param expressionVisitor The visitor for the enclosing expression.
     */
    public StringFunctionsVisitor(ExpressionVisitor expressionVisitor, GenericFunctionsVisitor genericFunctionsVisitor) {
        this.exprVisitor = Objects.requireNonNull(expressionVisitor);
        this.genericFunctionsVisitor = Objects.requireNonNull(genericFunctionsVisitor);
    }

    public static String trim(String value) {
        return value.trim();
    }

    public static String ltrim(String value) {
        return LTRIM.matcher(value).replaceAll("");
    }

    public static String rtrim(String value) {
        return RTRIM.matcher(value).replaceAll("");
    }

    public static String ucase(String value) {
        return value.toUpperCase();
    }

    public static String lcase(String value) {
        return value.toLowerCase();
    }

    public static Long len(String value) {
        if (value == null) {
            return null;
        }
        return (long) value.length();
    }

    public static String substring(String value, Long start, Long end) {
        return null;
    }

    /**
     * Visits expressions corresponding to unary string functions.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the result of the string function on the operand.
     */
    @Override
    public ResolvableExpression visitUnaryStringFunction(VtlParser.UnaryStringFunctionContext ctx) {
        try {
            var pos = fromContext(ctx);
            var parameters = List.of(exprVisitor.visit(ctx.expr()));
            switch (ctx.op.getType()) {
                case VtlParser.TRIM:
                    return genericFunctionsVisitor.invokeFunction("trim", parameters, pos);
                case VtlParser.LTRIM:
                    return genericFunctionsVisitor.invokeFunction("ltrim", parameters, pos);
                case VtlParser.RTRIM:
                    return genericFunctionsVisitor.invokeFunction("rtrim", parameters, pos);
                case VtlParser.UCASE:
                    return genericFunctionsVisitor.invokeFunction("ucase", parameters, pos);
                case VtlParser.LCASE:
                    return genericFunctionsVisitor.invokeFunction("lcase", parameters, pos);
                case VtlParser.LEN:
                    return genericFunctionsVisitor.invokeFunction("len", parameters, pos);
                default:
                    throw new UnsupportedOperationException("unknown operator " + ctx.op.getText());
            }
        } catch (VtlScriptException e) {
            throw new VtlRuntimeException(e);
        }

    }

    /**
     * Visits expressions corresponding to the substring function on a string operand.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the result of the substring function on the operand.
     */
    @Override
    public ResolvableExpression visitSubstrAtom(VtlParser.SubstrAtomContext ctx) {
        var pos = fromContext(ctx);
        try {
            if (ctx.children.size() > 8) {
                // Compute the amount of parameters (excluding the tokens etc.)
                String args = String.valueOf((ctx.children.size() - 4) / 2);
                throw new VtlRuntimeException(new InvalidArgumentException(
                        "too many args (" + args + ")", fromContext(ctx)));
            }
            // TODO: grammar issue: endParameter should be named lengthParameter
            ResolvableExpression expression = exprVisitor.visit(ctx.expr());
            ResolvableExpression startExpression = ctx.startParameter == null ?
                    ResolvableExpression.withType(Long.class).withPosition(pos).using(c -> 0L)
                    : exprVisitor.visit(ctx.startParameter).checkInstanceOf(Long.class);
            ResolvableExpression lengthExpression = ctx.endParameter == null ? null
                    : exprVisitor.visit(ctx.endParameter).checkInstanceOf(Long.class);

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
        } catch (InvalidTypeException e) {
            throw new VtlRuntimeException(e);
        }
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
        var pos = fromContext(ctx);
        try {
            ResolvableExpression expression = exprVisitor.visit(ctx.expr(0)).checkInstanceOf(String.class);
            ResolvableExpression pattern = exprVisitor.visit(ctx.pattern).checkInstanceOf(String.class);

            var start = ctx.startParameter == null ?
                    ResolvableExpression.withType(Long.class).withPosition(pos).using(c -> 0L)
                    : exprVisitor.visit(ctx.startParameter).checkInstanceOf(Long.class);

            var occurrence = ctx.occurrenceParameter == null ?
                    ResolvableExpression.withType(Long.class).withPosition(pos).using(c -> 1L)
                    : exprVisitor.visit(ctx.occurrenceParameter).checkInstanceOf(Long.class);

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
        } catch (InvalidTypeException e) {
            throw new VtlRuntimeException(e);
        }
    }
}
