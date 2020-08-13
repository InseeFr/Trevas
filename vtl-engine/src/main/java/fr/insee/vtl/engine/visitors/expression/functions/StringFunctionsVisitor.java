package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.exceptions.InvalidTypeException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.LongExpression;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.StringExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;
import java.util.regex.Pattern;

import static fr.insee.vtl.engine.utils.TypeChecking.assertString;

public class StringFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final Pattern LTRIM = Pattern.compile("^\\s+");
    private final Pattern RTRIM = Pattern.compile("\\s+$");

    private final ExpressionVisitor exprVisitor;

    public StringFunctionsVisitor(ExpressionVisitor expressionVisitor) {
        exprVisitor = Objects.requireNonNull(expressionVisitor);
    }

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
            return RTRIM.matcher(value).replaceAll("");
        });
    }

    private ResolvableExpression handleUCase(VtlParser.ExprContext expressionCtx) {
        ResolvableExpression expression = assertString(exprVisitor.visit(expressionCtx), expressionCtx);
        return StringExpression.of(context -> {
            String value = (String) expression.resolve(context);
            return value.toUpperCase();
        });
    }

    private ResolvableExpression handleLCase(VtlParser.ExprContext expressionCtx) {
        ResolvableExpression expression = assertString(exprVisitor.visit(expressionCtx), expressionCtx);
        return StringExpression.of(context -> {
            String value = (String) expression.resolve(context);
            return value.toLowerCase();
        });
    }

    private ResolvableExpression handleLen(VtlParser.ExprContext expressionCtx) {
        ResolvableExpression expression = assertString(exprVisitor.visit(expressionCtx), expressionCtx);
        return LongExpression.of(context -> {
            String value = (String) expression.resolve(context);
            return (long) value.length();
        });
    }

    private ResolvableExpression handleLTrim(VtlParser.ExprContext expressionCtx) {
        ResolvableExpression expression = assertString(exprVisitor.visit(expressionCtx), expressionCtx);
        return StringExpression.of(context -> {
            String value = (String) expression.resolve(context);
            return LTRIM.matcher(value).replaceAll("");
        });
    }

    private ResolvableExpression handleTrim(VtlParser.ExprContext expressionCtx) {
        ResolvableExpression expression = assertString(exprVisitor.visit(expressionCtx), expressionCtx);
        return StringExpression.of(context -> {
            String value = (String) expression.resolve(context);
            return value.trim();
        });
    }

    @Override
    public ResolvableExpression visitSubstrAtom(VtlParser.SubstrAtomContext ctx) {
        if (ctx.children.size() > 8) {
            String args = String.valueOf((ctx.children.size() - 4) / 2);
            // TODO: Define subclass of VtlScriptException.
            throw new UnsupportedOperationException("too many args (" + args + ") for: " + ctx.getText());
        }
        ResolvableExpression startExpression = null;
        ResolvableExpression endExpression = null;
        if (ctx.startParameter != null) {
            startExpression = exprVisitor.visit(ctx.startParameter);
        }
        if (ctx.endParameter != null) {
            endExpression = exprVisitor.visit(ctx.endParameter);
        }
        ResolvableExpression expression = exprVisitor.visit(ctx.expr());
        ResolvableExpression finalStartExpression = startExpression;
        ResolvableExpression finalEndExpression = endExpression;
        return ResolvableExpression.withType(String.class, context -> {
            String value = (String) expression.resolve(context);
            int startValue = finalStartExpression != null ? ((Long) finalStartExpression.resolve(context)).intValue() : 0;
            int endValue = finalEndExpression != null ? ((Long) finalEndExpression.resolve(context)).intValue() : value.length();
            return value.substring(startValue, endValue);
        });
    }

    @Override
    public ResolvableExpression visitReplaceAtom(VtlParser.ReplaceAtomContext ctx) {
        ResolvableExpression expression = exprVisitor.visit(ctx.expr(0));
        ResolvableExpression inputPattern = exprVisitor.visit(ctx.param);
        ResolvableExpression outputPattern = null;

        if (!inputPattern.getType().equals(String.class)) {
            throw new VtlRuntimeException(
                    new InvalidTypeException(String.class, inputPattern.getType(), ctx.param)
            );
        }

        if (ctx.optionalExpr() != null) {
            outputPattern =  exprVisitor.visit(ctx.optionalExpr());
            if (!outputPattern.getType().equals(String.class)) {
                throw new VtlRuntimeException(
                        new InvalidTypeException(String.class, outputPattern.getType(), ctx.optionalExpr())
                );
            }
        }

        ResolvableExpression finalOutputPattern = outputPattern;

        return ResolvableExpression.withType(String.class, context -> {
            String value = (String) expression.resolve(context);
            String inputPatternValue = (String) inputPattern.resolve(context);
            String outputPatternValue = finalOutputPattern != null ? (String) finalOutputPattern.resolve(context) : "";
            return value.replaceAll(inputPatternValue, outputPatternValue);
        });
    }

    @Override
    public ResolvableExpression visitInstrAtom(VtlParser.InstrAtomContext ctx) {
        ResolvableExpression expression = exprVisitor.visit(ctx.expr(0));
        ResolvableExpression pattern = exprVisitor.visit(ctx.pattern);
        ResolvableExpression start = null;
        ResolvableExpression occurence = null;

        if (!pattern.getType().equals(String.class)) {
            throw new VtlRuntimeException(
                    new InvalidTypeException(String.class, pattern.getType(), ctx.pattern)
            );
        }

        if (ctx.startParameter != null) {
            start = exprVisitor.visit(ctx.startParameter);
            if (!start.getType().equals(Long.class)) {
                throw new VtlRuntimeException(
                        new InvalidTypeException(Long.class, start.getType(), ctx.startParameter)
                );
            }
        }

        if (ctx.occurrenceParameter != null) {
            occurence = exprVisitor.visit(ctx.occurrenceParameter);
            if (!occurence.getType().equals(Long.class)) {
                throw new VtlRuntimeException(
                        new InvalidTypeException(Long.class, occurence.getType(), ctx.occurrenceParameter)
                );
            }
        }

        ResolvableExpression finalStart = start;
        ResolvableExpression finalOccurence = occurence;

        return ResolvableExpression.withType(Long.class, context -> {
            String value = (String) expression.resolve(context);
            String patternValue = (String) pattern.resolve(context);
            int startValue = finalStart != null ? ((Long) finalStart.resolve(context)).intValue() : 0;
            int occurenceValue = finalOccurence != null ? ((Long) finalOccurence.resolve(context)).intValue() : 1;
            return (long) StringUtils.ordinalIndexOf(value.substring(startValue), patternValue, occurenceValue) + 1L;
        });
    }

}
