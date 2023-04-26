package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        if (value == null) {
            return null;
        }
        return value.trim();
    }

    public static String ltrim(String value) {
        if (value == null) {
            return null;
        }
        return LTRIM.matcher(value).replaceAll("");
    }

    public static String rtrim(String value) {
        if (value == null) {
            return null;
        }
        return RTRIM.matcher(value).replaceAll("");
    }

    public static String ucase(String value) {
        if (value == null) {
            return null;
        }
        return value.toUpperCase();
    }

    public static String lcase(String value) {
        if (value == null) {
            return null;
        }
        return value.toLowerCase();
    }

    public static Long len(String value) {
        if (value == null) {
            return null;
        }
        return (long) value.length();
    }

    public static String substr(String value, Long start, Long len) {
        if (value == null || start == null || len == null) {
            return null;
        }
        if (start > value.length()) {
            return "";
        }
        if (start != 0) {
            start = start - 1;
        }

        var end = start + len;
        if (end > value.length()) {
            return value.substring(Math.toIntExact(start));
        }
        return value.substring(Math.toIntExact(start), Math.toIntExact(end));
    }

    public static String substr(String value, Long start) {
        if (value == null) {
            return null;
        }
        return substr(value, start, (long) value.length());
    }

    public static String substr(String value) {
        return value;
    }

    public static String replace(String value, String pattern, String replacement) {
        if (value == null || pattern == null || replacement == null) {
            return null;
        }
        return value.replaceAll(pattern, replacement);
    }

    public static String replace(String value, String pattern) {
        return replace(value, pattern, "");
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
        try {
            var parameters = Stream.of(ctx.expr(), ctx.startParameter, ctx.endParameter)
                    .filter(Objects::nonNull)
                    .map(exprVisitor::visit)
                    .collect(Collectors.toList());
            return genericFunctionsVisitor.invokeFunction("substr", parameters, fromContext(ctx));
        } catch (VtlScriptException e) {
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
        try {
            var parameters = Stream.of(ctx.expr(0), ctx.param, ctx.optionalExpr())
                    .filter(Objects::nonNull)
                    .map(exprVisitor::visit)
                    .collect(Collectors.toList());

            return genericFunctionsVisitor.invokeFunction("replace", parameters, fromContext(ctx));
        } catch (VtlScriptException e) {
            throw new VtlRuntimeException(e);
        }
    }


    public static Long instr(String v, String v2, Long a, Long b) {
        return 0L;
    }

    /**
     * Visits expressions corresponding to the pattern location function on a string operand.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the result of the pattern location function on the operand.
     */
    @Override
    public ResolvableExpression visitInstrAtom(VtlParser.InstrAtomContext ctx) {
        try {
            var parameters = Stream.of(ctx.startParameter, ctx.occurrenceParameter)
                    .filter(Objects::nonNull)
                    .map(exprVisitor::visit)
                    .collect(Collectors.toList());

            return genericFunctionsVisitor.invokeFunction("instr", parameters, fromContext(ctx));
        } catch (VtlScriptException e) {
            throw new VtlRuntimeException(e);
        }


//        var pos = fromContext(ctx);
//        try {
//            ResolvableExpression expression = exprVisitor.visit(ctx.expr(0)).checkInstanceOf(String.class);
//            ResolvableExpression pattern = exprVisitor.visit(ctx.pattern).checkInstanceOf(String.class);
//
//            var start = ctx.startParameter == null ?
//                    ResolvableExpression.withType(Long.class).withPosition(pos).using(c -> 0L)
//                    : exprVisitor.visit(ctx.startParameter).checkInstanceOf(Long.class);
//
//            var occurrence = ctx.occurrenceParameter == null ?
//                    ResolvableExpression.withType(Long.class).withPosition(pos).using(c -> 1L)
//                    : exprVisitor.visit(ctx.occurrenceParameter).checkInstanceOf(Long.class);
//
//            return ResolvableExpression.withType(Long.class).withPosition(pos).using(
//                    context -> {
//                        String value = (String) expression.resolve(context);
//                        String patternValue = (String) pattern.resolve(context);
//                        Long startValue = (Long) start.resolve(context);
//                        Long occurrenceValue = (Long) occurrence.resolve(context);
//                        if (TypeChecking.hasNullArgs(value, patternValue, startValue, occurrenceValue)) return null;
//                        return StringUtils.ordinalIndexOf(value.substring(startValue.intValue()), patternValue, occurrenceValue.intValue()) + 1L;
//                    }
//            );
//        } catch (InvalidTypeException e) {
//            throw new VtlRuntimeException(e);
//        }
    }
}
