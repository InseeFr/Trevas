package fr.insee.vtl.engine.utils;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ResolvableExpression;
import java.util.List;
import org.junit.jupiter.api.Test;

public class TypeCheckingUtilsTest {

  Positioned fakePos =
      new Positioned() {
        @Override
        public Position getPosition() {
          return new Position(1, 1, 1, 1);
        }
      };

  private ResolvableExpression expr(Class<?> type) {
    return ResolvableExpression.withType(type)
        // return fakePos, value is not read
        .withPosition(fakePos)
        // return always null, value is not read
        .using(c -> null);
  }

  @Test
  void testAllNull() {
    var list = List.of(expr(null), expr(null));
    assertTrue(TypeChecking.hasSameTypeOrNumberOrNull(list));
  }

  @Test
  void testAllSameType() {
    var list = List.of(expr(String.class), expr(String.class));
    assertTrue(TypeChecking.hasSameTypeOrNumberOrNull(list));
  }

  @Test
  void testDifferentTypes() {
    var list = List.of(expr(String.class), expr(Integer.class));
    assertFalse(TypeChecking.hasSameTypeOrNumberOrNull(list));
  }

  @Test
  void testAllNumbersSameSubclass() {
    var list = List.of(expr(Integer.class), expr(Integer.class));
    assertTrue(TypeChecking.hasSameTypeOrNumberOrNull(list));
  }

  @Test
  void testAllNumbersDifferentSubclass() {
    var list = List.of(expr(Integer.class), expr(Double.class), expr(Long.class));
    assertTrue(TypeChecking.hasSameTypeOrNumberOrNull(list));
  }

  @Test
  void testMixNullAndNumbers() {
    var list = List.of(expr(null), expr(Integer.class), expr(Double.class));
    assertTrue(TypeChecking.hasSameTypeOrNumberOrNull(list));
  }

  @Test
  void testMixNullAndOtherType() {
    var list = List.of(expr(null), expr(String.class));
    assertTrue(TypeChecking.hasSameTypeOrNumberOrNull(list));
  }
}
