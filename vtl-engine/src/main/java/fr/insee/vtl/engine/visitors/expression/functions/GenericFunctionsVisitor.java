package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.exceptions.FunctionNotFoundException;
import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.expressions.DatasetFunctionExpression;
import fr.insee.vtl.engine.expressions.FunctionExpression;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

/**
 * <code>GenericFunctionsVisitor</code> is the base visitor for cast expressions.
 */
public class GenericFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final VtlScriptEngine engine;
    private final ExpressionVisitor exprVisitor;

    /**
     * Constructor taking an expression visitor.
     *
     * @param expressionVisitor The visitor for the enclosing expression.
     * @param engine            The {@link VtlScriptEngine}.
     */
    public GenericFunctionsVisitor(ExpressionVisitor expressionVisitor, VtlScriptEngine engine) {
        this.engine = Objects.requireNonNull(engine);
        exprVisitor = Objects.requireNonNull(expressionVisitor);
    }

    /**
     * Method to map basic scalar types and classes.
     *
     * @param basicScalarType Basic scalar type.
     * @param basicScalarText Basic scalar text.
     */
    private static Class<?> getOutputClass(Integer basicScalarType, String basicScalarText) {
        switch (basicScalarType) {
            case VtlParser.STRING:
                return String.class;
            case VtlParser.INTEGER:
                return Long.class;
            case VtlParser.NUMBER:
                return Double.class;
            case VtlParser.BOOLEAN:
                return Boolean.class;
            case VtlParser.DATE:
                return Instant.class;
            default:
                throw new UnsupportedOperationException("basic scalar type " + basicScalarText + " unsupported");
        }
    }

    public ResolvableExpression invokeFunction(String funcName, List<ResolvableExpression> parameters, Positioned position) throws VtlScriptException {
        // TODO: Use parameters to find functions so we can override them.
        var method = engine.findMethod(funcName);
        if (method.isEmpty()) {
            throw new FunctionNotFoundException(funcName, position);
        }
        var parameterTypes = parameters.stream().map(ResolvableExpression::getType).collect(Collectors.toList());
        // TODO: Method with more that one dataset parameters.
        //          ie: parameterTypes.contains(Dataset.class)
        if (parameterTypes.size() == 1 && parameterTypes.get(0).equals(Dataset.class)) {
            return new DatasetFunctionExpression(method.get(), (DatasetExpression) parameters.get(0), position);
        } else {
            return new FunctionExpression(method.get(), parameters, position);
        }
    }


    @Override
    public ResolvableExpression visitCallDataset(VtlParser.CallDatasetContext ctx) {
        // Strange name, this is the generic function syntax; fnName ( param, * ).
        try {
            List<ResolvableExpression> parameters = ctx.parameter().stream().
                    map(exprVisitor::visit)
                    .collect(Collectors.toList());
            return invokeFunction(ctx.operatorID().getText(), parameters, fromContext(ctx));
        } catch (VtlScriptException e) {
            throw new VtlRuntimeException(e);
        }
    }

    /**
     * Visits expressions with cast operators.
     *
     * @param ctx The scripting context for the expression.
     * @return A <code>ResolvableExpression</code> resolving to the result of the cast operation.
     */
    @Override
    public ResolvableExpression visitCastExprDataset(VtlParser.CastExprDatasetContext ctx) {
        ResolvableExpression expression = exprVisitor.visit(ctx.expr());
        TerminalNode maskNode = ctx.STRING_CONSTANT();
        // STRING_CONSTANT().getText return null or a string wrapped by quotes
        String mask = maskNode == null ? null :
                maskNode.getText()
                        .replaceAll("\"", "")
                        .replace("YYYY", "yyyy")
                        .replace("DD", "dd");
        Token symbol = ((TerminalNode) ctx.basicScalarType().getChild(0)).getSymbol();
        Integer basicScalarType = symbol.getType();
        String basicScalarText = symbol.getText();

        Class<?> outputClass = getOutputClass(basicScalarType, basicScalarText);

        if (Object.class.equals(expression.getType())) {
            return ResolvableExpression.ofType(outputClass, null);
        }
        if (String.class.equals(expression.getType())) {
            return StringExpression.castTo(expression, outputClass, mask);
            // Antlr context is not serializable
            // TODO: Find a way to make ctx serializable
            //        .handleException(NumberFormatException.class, nfe -> new VtlRuntimeException(
            //                new InvalidArgumentException("cannot cast to number: " + nfe.getMessage(), ctx)));
        }
        if (Boolean.class.equals(expression.getType())) {
            return BooleanExpression.castTo(expression, outputClass);
        }
        if (Long.class.equals(expression.getType())) {
            return LongExpression.castTo(expression, outputClass);
        }
        if (Double.class.equals(expression.getType())) {
            return DoubleExpression.castTo(expression, outputClass);
        }
        if (Instant.class.equals(expression.getType())) {
            if (mask == null || mask.isEmpty()) {
                throw new VtlRuntimeException(new InvalidArgumentException("cannot cast date: no mask specified", fromContext(ctx)));
            }
            return InstantExpression.castTo(expression, outputClass, mask);
        }
        throw new UnsupportedOperationException("cast unsupported on expression of type: " + expression.getType());
    }

}
