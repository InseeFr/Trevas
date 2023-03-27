package fr.insee.vtl.model;

import fr.insee.vtl.model.exceptions.InvalidTypeException;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * <code>ResolvableExpression</code> is the base interface for VTL expressions that can be resolved in a given context.
 */
public abstract class ResolvableExpression implements TypedExpression, Positioned, Serializable {

    private final Position position;

    protected ResolvableExpression(Positioned positioned) {
        this(Objects.requireNonNull(positioned).getPosition());
    }

    protected ResolvableExpression(Position position) {
        this.position = Objects.requireNonNull(position);
    }

    @Override
    public Position getPosition() {
        return position;
    }

    public static class Builder<T> {
        private final Class<T> type;
        private VtlFunction<Map<String, Object>, T> func;
        private Positioned position;

        Builder(Class<T> type) {
            this.type = type;
        }

        public Builder<T> withPosition(Positioned position) {
            this.position = position;
            return this;
        }

        public ResolvableExpression using(VtlFunction<Map<String, Object>, T> function) {
            return new ResolvableExpression(position) {
                @Override
                public Object resolve(Map<String, Object> context) {
                    return function.apply(context);
                }

                @Override
                public Class<?> getType() {
                    return type;
                }
            };
        }
    }

    public static <T> Builder<T> withType(Class<T> type) {
        return new Builder<T>(type);
    }

    /**
     * Checks that the type of the expression is either the same as, or is a superclass or superinterface of, the class
     * or interface represented by the specified Class parameter.
     */
    public ResolvableExpression checkAssignableFrom(Class<?> clazz) throws InvalidTypeException {
        if (!getType().isAssignableFrom(clazz)) {
            throw new InvalidTypeException(clazz, getType(), this);
        }
        return this;
    }

    /**
     * Checks that the type of the class is either the same as, or is a superclass or superinterface of, the class
     * or interface of the expression.
     */
    public ResolvableExpression checkAssignableTo(Class<?> clazz) throws InvalidTypeException {
        if (Object.class.equals(this.getType())) {
            return ResolvableExpression.withType(Class.class).withPosition(this).using(ctx -> null);
        }
        if (!clazz.isAssignableFrom(this.getType())) {
            throw new InvalidTypeException(clazz, getType(), this);
        }
        return this;
    }

    /**
     * Returns a <code>ResolvableExpression</code> with a given type and resolution function.
     *
     * @param clazz The <code>Class</code> corresponding to the type of the expression to create.
     * @param func  The resolution function for the expression to create.
     * @param <T>   The type of the expression to create.
     * @return An instance of <code>ResolvableExpression</code> with the given type and resolution function.
     */
    @Deprecated
    public static <T> ResolvableExpression withType(Class<T> clazz, VtlFunction<Map<String, Object>, T> func) {
        return withType(() -> {
            throw new UnsupportedOperationException("TODO");
        }, clazz, func);
    }

    @Deprecated
    public static <T> ResolvableExpression withType(Positioned pos, Class<T> clazz, VtlFunction<Map<String, Object>, T> func) {
        return new ResolvableExpression(pos) {

            @Override
            public Object resolve(Map<String, Object> context) {
                return func.apply(context);
            }

            @Override
            public Class<T> getType() {
                return clazz;
            }

        };
    }

    /**
     * Returns a <code>ResolvableExpression</code> with a given type and resolution function.
     *
     * @param clazz The <code>Class</code> corresponding to the type of the expression to create.
     * @param func  The resolution function for the expression to create.
     * @param <T>   The type of the expression to create.
     * @return An instance of <code>ResolvableExpression</code> with the given type and resolution function.
     */
    @Deprecated
    public static <T> ResolvableExpression withTypeCasting(Class<T> clazz,
                                                           VtlBiFunction<Class<T>, Map<String, Object>, T> func) {
        return withTypeCasting(() -> {
            throw new UnsupportedOperationException("TODO");
        }, clazz, func);
    }

    @Deprecated
    public static <T> ResolvableExpression withTypeCasting(Positioned position, Class<T> clazz,
                                                           VtlBiFunction<Class<T>, Map<String, Object>, T> func) {
        return new ResolvableExpression(position) {

            @Override
            public Object resolve(Map<String, Object> context) {
                return func.apply(clazz, context);
            }

            @Override
            public Class<T> getType() {
                return clazz;
            }

        };
    }

    /**
     * Returns a <code>ResolvableExpression</code> with a given type and value.
     *
     * @param clazz The <code>Class</code> corresponding to the type of the expression to create.
     * @param value The expression value.
     * @return An instance of <code>ResolvableExpression</code> with the given type and value.
     */
    @Deprecated
    public static ResolvableExpression ofType(Class<?> clazz, Object value) {
        return new ResolvableExpression(() -> {
            throw new UnsupportedOperationException("TODO");
        }) {

            @Override
            public Object resolve(Map<String, Object> context) {
                return value;
            }

            @Override
            public Class getType() {
                return clazz;
            }

        };
    }

    /**
     * Resolves the expression in a given context.
     *
     * @param context The context for the resolution.
     * @return The result of the resolution of the expression in the given context.
     */
    public abstract Object resolve(Map<String, Object> context);

    /**
     * Resolves the expression for a given datapoint.
     *
     * @param context the data point to resolve the expression against
     * @return the result of the resolution of the expression
     */
    public Object resolve(Structured.DataPoint context) {
        return resolve(new Structured.DataPointMap(context));
    }
}
