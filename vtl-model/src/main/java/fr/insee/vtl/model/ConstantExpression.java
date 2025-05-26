package fr.insee.vtl.model;

import java.util.Map;

public class ConstantExpression extends ResolvableExpression {

  private final Object value;

  public ConstantExpression(Object value, Positioned position) {
    super(position);
    this.value = value;
  }

  @Override
  public Object resolve(Map<String, Object> context) {
    return value;
  }

  @Override
  public Class<?> getType() {
    // TODO: Use expr == Constant.NULL instead.
    if (value == null) {
      return Object.class;
    }
    return value.getClass();
  }
}
