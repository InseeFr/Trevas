package fr.insee.vtl.prov.utils;

import java.time.Instant;
import org.threeten.extra.Interval;
import org.threeten.extra.PeriodDuration;

public class VtlTypes {

  /**
   * Method to map Java classes and VTL basic scalar types.
   *
   * @param clazz Java type.
   * @return Basic scalar type
   */
  public static String getVtlType(Class<?> clazz) {
    if (clazz.equals(String.class)) {
      return "STRING";
    } else if (clazz.equals(Long.class)) {
      return "INTEGER";
    } else if (clazz.equals(Double.class)) {
      return "NUMBER";
    } else if (clazz.equals(Boolean.class)) {
      return "BOOLEAN";
    } else if (clazz.equals(Instant.class)) {
      return "DATE";
    } else if (clazz.equals(PeriodDuration.class)) {
      return "DURATION";
    } else if (clazz.equals(Interval.class)) {
      return "TIME_PERIOD";
    }
    throw new UnsupportedOperationException("class " + clazz + " unsupported");
  }
}
