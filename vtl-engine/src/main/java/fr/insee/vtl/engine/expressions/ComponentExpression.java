package fr.insee.vtl.engine.expressions;

import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.Structured;
import java.util.Map;
import java.util.Objects;

// TODO: Extract to model
// TODO: Check that we don't already have something like that.
public class ComponentExpression extends ResolvableExpression {

  private final Structured.Component component;

  public ComponentExpression(Structured.Component component, Positioned position) {
    super(position);
    this.component = Objects.requireNonNull(component);
  }

  @Override
  public Object resolve(Map<String, Object> context) {
    return context.get(component.getName());
  }

  @Override
  public Class<?> getType() {
    return component.getType();
  }
}
