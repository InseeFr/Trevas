package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.exceptions.UndefinedVariableException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.expressions.ComponentExpression;
import fr.insee.vtl.model.*;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

/**
 * <code>VarIdVisitor</code> is the base visitor for variable identifiers.
 */
public class VarIdVisitor extends VtlBaseVisitor<ResolvableExpression> implements Serializable {

    private final Map<String, Object> context;

    /**
     * Constructor taking a scripting context.
     *
     * @param context The context for the visitor.
     */
    public VarIdVisitor(Map<String, Object> context) {
        this.context = Objects.requireNonNull(context);
    }

    @Override
    public ResolvableExpression visitVarID(VtlParser.VarIDContext ctx) {
        final String variableName = ctx.getText();
        Positioned pos = fromContext(ctx);

        if (!context.containsKey(variableName)) {
            throw new VtlRuntimeException(new UndefinedVariableException(variableName, pos));
        }

        Object value = context.get(variableName);
        if (value instanceof Dataset) {
            return DatasetExpression.of((Dataset) value, pos);
        }

        if (value instanceof Structured.Component) {
            Structured.Component component = (Structured.Component) value;
            return new ComponentExpression(component, pos);
        }

        if (value instanceof Integer) {
            value = Long.valueOf((Integer) value);
        }

        if (value instanceof Float) {
            value = Double.valueOf((Float) value);
        }

        if (value == null) {
            return ResolvableExpression.withType(Object.class).withPosition(pos).using(c -> c.get(variableName));
        }

        return new ConstantExpression(value, pos);
    }
}
