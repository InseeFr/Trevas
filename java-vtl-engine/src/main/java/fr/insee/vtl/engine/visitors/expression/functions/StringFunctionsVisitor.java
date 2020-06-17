package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.util.regex.Pattern;

public class StringFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor = new ExpressionVisitor();
    private final static Pattern LTRIM = Pattern.compile("^\\s+");
    private final static Pattern RTRIM = Pattern.compile("\\s+$");

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

    // visitReplaceAtom

    // visitInstrAtom

    // visitSubstrAtom
}
