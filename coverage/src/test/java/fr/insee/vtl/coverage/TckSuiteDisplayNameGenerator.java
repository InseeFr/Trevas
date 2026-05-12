package fr.insee.vtl.coverage;

import java.lang.reflect.Method;
import org.junit.jupiter.api.DisplayNameGenerator;

/**
 * JUnit suite title from {@code -Dtck.suite.title=…} (set by Maven profile in CI for Spark 3 vs 4).
 */
public final class TckSuiteDisplayNameGenerator implements DisplayNameGenerator {

  private static final DisplayNameGenerator DEFAULT = DisplayNameGenerator.Standard.INSTANCE;

  @Override
  public String generateDisplayNameForClass(Class<?> testClass) {
    if (testClass == TCKTest.class) {
      return System.getProperty("tck.suite.title", "TCK VTL v2.1 (Spark 3)");
    }
    return DEFAULT.generateDisplayNameForClass(testClass);
  }

  @Override
  public String generateDisplayNameForNestedClass(Class<?> nestedClass) {
    return DEFAULT.generateDisplayNameForNestedClass(nestedClass);
  }

  @Override
  public String generateDisplayNameForMethod(Class<?> testClass, Method testMethod) {
    return DEFAULT.generateDisplayNameForMethod(testClass, testMethod);
  }
}
