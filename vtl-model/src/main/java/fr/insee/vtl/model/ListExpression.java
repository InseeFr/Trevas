package fr.insee.vtl.model;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * The <code>ListExpression</code> class is an abstract representation of an expression of type
 * <code>List</code>.
 */
public abstract class ListExpression extends ResolvableExpression
    implements TypedContainerExpression {

  public ListExpression(Positioned position) {
    super(position);
  }

  /**
   * Constructor taking a collection of elements and a type for these elements.
   *
   * @param elements The collection of elements to include in the list expression.
   * @param containedType The type for the list elements.
   * @return A new list expression resolving to a list with the elements and type provided.
   */
  public static <T> ListExpression withContainedType(
      Collection<Object> elements, Class<T> containedType, Positioned position) {
    List<Object> list = List.copyOf(elements);
    return new ListExpression(position) {
      @Override
      public List<?> resolve(Map<String, Object> context) {
        return list;
      }

      @Override
      public Class<?> containedType() {
        return containedType;
      }
    };
  }

  @Override
  public Class<?> getType() {
    return List.class;
  }

  @Override
  public abstract List<?> resolve(Map<String, Object> context);
}
