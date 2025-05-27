package fr.insee.vtl.model;

import fr.insee.vtl.model.exceptions.InvalidTypeException;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * <code>ResolvableExpression</code> is the base interface for VTL expressions that can be resolved
 * in a given context.
 */
public abstract class ResolvableExpression implements TypedExpression, Positioned, Serializable {

  private final Position position;

  protected ResolvableExpression(Positioned positioned) {
    this(Objects.requireNonNull(positioned).getPosition());
  }

  protected ResolvableExpression(Position position) {
    this.position = Objects.requireNonNull(position);
  }

  public static <T> Builder<T> withType(Class<T> type) {
    return new Builder<>(type);
  }

  @Override
  public Position getPosition() {
    return position;
  }

  /**
   * Checks that the type of the class is either the same as, or is a superclass or superinterface
   * of, the class or interface of the expression.
   */
  public ResolvableExpression checkInstanceOf(Class<?> clazz) throws InvalidTypeException {
    if (Object.class.equals(this.getType())) {
      return ResolvableExpression.withType(Object.class).withPosition(this).using(ctx -> null);
    }
    if (!clazz.isAssignableFrom(this.getType())) {
      throw new InvalidTypeException(clazz, getType(), this);
    }
    return this;
  }

  public <T> ResolvableExpression tryCast(Class<T> clazz) {
    if (Object.class.equals(this.getType())) {
      return ResolvableExpression.withType(clazz).withPosition(this).using(ctx -> null);
    }
    return ResolvableExpression.withType(clazz)
        .withPosition(this)
        .using(
            ctx -> {
              var value = this.resolve(ctx);
              try {
                return clazz.cast(value);
              } catch (ClassCastException cce) {
                throw new RuntimeException(new VtlScriptException(cce, this));
              }
            });
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

  public static class Builder<T> implements Serializable {
    private final Class<T> type;
    private Position position;

    Builder(Class<T> type) {
      this.type = type;
    }

    public Builder<T> withPosition(Positioned positioned) {
      return withPosition(positioned.getPosition());
    }

    public Builder<T> withPosition(Position position) {
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
}
