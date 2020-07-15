package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import javax.script.ScriptContext;
import java.util.List;
import java.util.regex.Pattern;

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

    // visitReplaceAtom

    // visitInstrAtom
}
