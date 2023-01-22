package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.exceptions.UndefinedVariableException;
import fr.insee.vtl.engine.exceptions.UnsupportedTypeException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.model.BooleanExpression;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.DoubleExpression;
import fr.insee.vtl.model.InstantExpression;
import fr.insee.vtl.model.LongExpression;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.StringExpression;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

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

        if (!context.containsKey(variableName)) {
            throw new VtlRuntimeException(new UndefinedVariableException(ctx));
        }

        Object value = context.get(variableName);
        if (value instanceof Dataset) {
            return DatasetExpression.of((Dataset) value);
        }

        if (value instanceof Structured.Component) {
            var component = (Structured.Component) value;
            String name = component.getName();
            Class type = component.getType();
            return ResolvableExpression.withType(type, c -> c.get(name));
        }

        if (value instanceof Integer || value instanceof Long) {
            return LongExpression.of(((Number) value).longValue());
        }

        if (value instanceof Float || value instanceof Double) {
            return DoubleExpression.of(((Number) value).doubleValue());
        }

        if (value instanceof Boolean) {
            return BooleanExpression.of((Boolean) value);
        }

        if (value instanceof CharSequence) {
            return StringExpression.of((CharSequence) value);
        }

        if (value instanceof Instant) {
            return InstantExpression.of((Instant) value);
        }

        if (value == null) {
            return ResolvableExpression.ofType(Object.class, null);
        }

        throw new VtlRuntimeException(new UnsupportedTypeException(value.getClass(), ctx));
    }
}
