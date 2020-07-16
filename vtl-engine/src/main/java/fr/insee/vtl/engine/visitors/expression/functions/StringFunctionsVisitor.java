package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.exceptions.InvalidTypeException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import javax.script.ScriptContext;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

public class StringFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final static Pattern LTRIM = Pattern.compile("^\\s+");
    private final static Pattern RTRIM = Pattern.compile("\\s+$");

    private final ExpressionVisitor exprVisitor;

    public StringFunctionsVisitor(ScriptContext context) {
        exprVisitor = new ExpressionVisitor(context);;
    }

    public ResolvableExpression visitUnaryStringFunction(VtlParser.UnaryStringFunctionContext ctx) {
        // TODO: deal with Long & Double dynamically
        ResolvableExpression expression = exprVisitor.visit(ctx.expr());
        switch (ctx.op.getType()) {
            case VtlParser.TRIM:
                return ResolvableExpression.withType(String.class, context -> {
                    String value = (String) expression.resolve(context);
                    return value.trim();
                });
            case VtlParser.LTRIM:
                return ResolvableExpression.withType(String.class, context -> {
                    String value = (String) expression.resolve(context);
                    return LTRIM.matcher(value).replaceAll("");
                });
            case VtlParser.RTRIM:
                return ResolvableExpression.withType(String.class, context -> {
                    String value = (String) expression.resolve(context);
                    return RTRIM.matcher(value).replaceAll("");
                });
            case VtlParser.UCASE:
                return ResolvableExpression.withType(String.class, context -> {
                    String value = (String) expression.resolve(context);
                    return value.toUpperCase();
                });
            case VtlParser.LCASE:
                return ResolvableExpression.withType(String.class, context -> {
                    String value = (String) expression.resolve(context);
                    return value.toLowerCase();
                });
            case VtlParser.LEN:
                return ResolvableExpression.withType(Long.class, context -> {
                    String value = (String) expression.resolve(context);
                    return Long.valueOf(value.length());
                });
            default:
                throw new UnsupportedOperationException("unknown operator " + ctx);
        }
    }

    public ResolvableExpression visitSubstrAtom(VtlParser.SubstrAtomContext ctx) {
        if (ctx.children.size() > 8) {
            String args = String.valueOf((ctx.children.size() - 4) / 2);
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

    public ResolvableExpression visitReplaceAtom(VtlParser.ReplaceAtomContext ctx) {
        ResolvableExpression expression = exprVisitor.visit(ctx.expr(0));
        ResolvableExpression inputPattern = exprVisitor.visit(ctx.param);
        ResolvableExpression outputPattern = null;

        if (!inputPattern.getType().equals(String.class)) {
            throw new VtlRuntimeException(
                    new InvalidTypeException(ctx.param, String.class, inputPattern.getType())
            );
        }

        if (ctx.optionalExpr() != null) {
            outputPattern =  exprVisitor.visit(ctx.optionalExpr());
            if (!outputPattern.getType().equals(String.class)) {
                throw new VtlRuntimeException(
                        new InvalidTypeException(ctx.optionalExpr(), String.class, outputPattern.getType())
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

    public ResolvableExpression visitInstrAtom(VtlParser.InstrAtomContext ctx) {
        ResolvableExpression expression = exprVisitor.visit(ctx.expr(0));
        ResolvableExpression pattern = exprVisitor.visit(ctx.pattern);
        ResolvableExpression start = null;
        ResolvableExpression occurence = null;

        if (!pattern.getType().equals(String.class)) {
            throw new VtlRuntimeException(
                    new InvalidTypeException(ctx.pattern, String.class, pattern.getType())
            );
        }

        if (ctx.startParameter != null) {
            start = exprVisitor.visit(ctx.startParameter);
            if (!start.getType().equals(Long.class)) {
                throw new VtlRuntimeException(
                        new InvalidTypeException(ctx.startParameter, Long.class, start.getType())
                );
            }
        }

        if (ctx.occurrenceParameter != null) {
            occurence = exprVisitor.visit(ctx.occurrenceParameter);
            if (!occurence.getType().equals(Long.class)) {
                throw new VtlRuntimeException(
                        new InvalidTypeException(ctx.occurrenceParameter, Long.class, occurence.getType())
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
            return Long.valueOf(StringUtils.ordinalIndexOf(value.substring(startValue), patternValue, occurenceValue) + 1);
        });
    }

}
