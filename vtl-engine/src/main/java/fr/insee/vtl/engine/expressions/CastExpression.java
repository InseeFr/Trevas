package fr.insee.vtl.engine.expressions;

import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;
import org.threeten.extra.Interval;
import org.threeten.extra.PeriodDuration;

public class CastExpression extends ResolvableExpression {

  private final Class<?> target;
  private final ResolvableExpression expr;

  private final String isNotSupported = " is not supported";

  public CastExpression(
      Positioned position, ResolvableExpression expr, String mask, Class<?> target)
      throws VtlScriptException {
    super(position);
    this.target = target;
    var source = expr.getType();
    if (source.equals(target)) {
      this.expr = expr;
    } else {
      if (String.class.equals(source)) {
        this.expr = castString(expr, mask);
      } else if (Boolean.class.equals(source)) {
        this.expr = castBoolean(expr);
      } else if (Long.class.equals(source)) {
        this.expr = castLong(expr);
      } else if (Double.class.equals(source)) {
        this.expr = castDouble(expr);
      } else if (Instant.class.equals(source)) {
        if (mask == null || mask.isEmpty()) {
          throw new InvalidArgumentException("cannot cast date: no mask specified", position);
        }
        this.expr = castInstant(expr, mask);
      } else
        throw new VtlScriptException(
            "cast unsupported on expression of type: " + expr.getType(), position);
    }
  }

  public ResolvableExpression castBoolean(ResolvableExpression expr) {
    var outputClass = getType();
    if (outputClass.equals(String.class)) {
      return ResolvableExpression.withType(String.class)
          .withPosition(expr)
          .using(
              context -> {
                Boolean exprValue = (Boolean) expr.resolve(context);
                if (exprValue == null) return null;
                return exprValue.toString();
              });
    } else if (outputClass.equals(Long.class)) {
      return ResolvableExpression.withType(Long.class)
          .withPosition(expr)
          .using(
              context -> {
                Boolean exprValue = (Boolean) expr.resolve(context);
                if (exprValue == null) return null;
                return exprValue ? 1L : 0L;
              });
    } else if (outputClass.equals(Double.class)) {
      return ResolvableExpression.withType(Double.class)
          .withPosition(expr)
          .using(
              context -> {
                Boolean exprValue = (Boolean) expr.resolve(context);
                if (exprValue == null) return null;
                return exprValue ? 1D : 0D;
              });
    }
    throw new ClassCastException("Cast Boolean to " + outputClass + isNotSupported);
  }

  private ResolvableExpression castDouble(ResolvableExpression expr) {
    var outputClass = getType();
    if (outputClass.equals(String.class))
      return ResolvableExpression.withType(String.class)
          .withPosition(expr)
          .using(
              context -> {
                Double exprValue = (Double) expr.resolve(context);
                if (exprValue == null) return null;
                return exprValue.toString();
              });
    if (outputClass.equals(Long.class))
      return ResolvableExpression.withType(Long.class)
          .withPosition(expr)
          .using(
              context -> {
                Double exprValue = (Double) expr.resolve(context);
                if (exprValue == null) return null;
                if (exprValue % 1 != 0)
                  throw new UnsupportedOperationException(
                      exprValue + " can not be casted into integer");
                return exprValue.longValue();
              });
    if (outputClass.equals(Double.class))
      return ResolvableExpression.withType(Double.class)
          .withPosition(expr)
          .using(context -> (Double) expr.resolve(context));
    if (outputClass.equals(Boolean.class))
      return ResolvableExpression.withType(Boolean.class)
          .withPosition(expr)
          .using(
              context -> {
                Double exprValue = (Double) expr.resolve(context);
                if (exprValue == null) return null;
                return !exprValue.equals(0D);
              });
    throw new ClassCastException("Cast Double to " + outputClass + isNotSupported);
  }

  private ResolvableExpression castInstant(ResolvableExpression expr, String mask) {
    var outputClass = getType();
    if (outputClass.equals(String.class))
      return ResolvableExpression.withType(String.class)
          .withPosition(expr)
          .using(
              context -> {
                var value = expr.resolve(context);
                Instant exprValue;
                if (value instanceof LocalDate date) {
                  exprValue = date.atStartOfDay().toInstant(ZoneOffset.UTC);
                } else {
                  exprValue = (Instant) value;
                }
                if (exprValue == null) return null;
                DateTimeFormatter maskFormatter = DateTimeFormatter.ofPattern(mask);
                return maskFormatter.format(exprValue.atOffset(ZoneOffset.UTC));
              });
    throw new ClassCastException("Cast Date to " + outputClass + isNotSupported);
  }

  private ResolvableExpression castLong(ResolvableExpression expr) {
    var outputClass = getType();
    if (outputClass.equals(String.class))
      return ResolvableExpression.withType(String.class)
          .withPosition(expr)
          .using(
              context -> {
                Long exprValue = (Long) expr.resolve(context);
                if (exprValue == null) return null;
                return exprValue.toString();
              });
    if (outputClass.equals(Double.class))
      return ResolvableExpression.withType(Double.class)
          .withPosition(expr)
          .using(
              context -> {
                Long exprValue = (Long) expr.resolve(context);
                if (exprValue == null) return null;
                return Double.valueOf(exprValue);
              });
    if (outputClass.equals(Boolean.class))
      return ResolvableExpression.withType(Boolean.class)
          .withPosition(expr)
          .using(
              context -> {
                Long exprValue = (Long) expr.resolve(context);
                if (exprValue == null) return null;
                return !exprValue.equals(0L);
              });
    throw new ClassCastException("Cast Long to " + outputClass + isNotSupported);
  }

  private ResolvableExpression castString(ResolvableExpression expr, String mask) {
    var outputClass = getType();
    if (outputClass.equals(Long.class)) {
      return ResolvableExpression.withType(Long.class)
          .withPosition(expr)
          .using(
              context -> {
                String exprValue = (String) expr.resolve(context);
                if (exprValue == null) return null;
                return Long.valueOf(exprValue);
              });
    } else if (outputClass.equals(Double.class)) {
      return ResolvableExpression.withType(Double.class)
          .withPosition(expr)
          .using(
              context -> {
                String exprValue = (String) expr.resolve(context);
                if (exprValue == null) return null;
                return Double.valueOf(exprValue);
              });
    } else if (outputClass.equals(Boolean.class)) {
      return ResolvableExpression.withType(Boolean.class)
          .withPosition(expr)
          .using(
              context -> {
                String exprValue = (String) expr.resolve(context);
                if (exprValue == null) return null;
                return Boolean.valueOf(exprValue);
              });
    } else if (outputClass.equals(Instant.class)) {
      return ResolvableExpression.withType(Instant.class)
          .withPosition(expr)
          .using(
              context -> {
                if (mask == null) return null;
                String exprValue = (String) expr.resolve(context);
                if (exprValue == null) return null;
                // The spec is pretty vague about date and time. Apparently, date is a point in time
                // so a good java
                // representation is Instant. But date can be created using only year/month and date
                // mask, leaving
                // any time information.
                DateTimeFormatter maskFormatter =
                    DateTimeFormatter.ofPattern(mask).withZone(ZoneOffset.UTC);
                try {
                  return LocalDateTime.parse(exprValue, maskFormatter).toInstant(ZoneOffset.UTC);
                } catch (DateTimeParseException dtp) {
                  return LocalDate.parse(exprValue, maskFormatter)
                      .atStartOfDay()
                      .toInstant(ZoneOffset.UTC);
                }
              });
    } else if (outputClass.equals(PeriodDuration.class)) {
      return ResolvableExpression.withType(PeriodDuration.class)
          .withPosition(expr)
          .using(
              context -> {
                String value = (String) expr.tryCast(String.class).resolve(context);
                return PeriodDuration.parse(value).normalizedYears().normalizedStandardDays();
              });
    } else if (outputClass.equals(Interval.class)) {
      return ResolvableExpression.withType(Interval.class)
          .withPosition(expr)
          .using(
              context -> {
                String value = (String) expr.tryCast(String.class).resolve(context);
                return Interval.parse(value);
              });
    } else {
      throw new ClassCastException("Cast String to " + outputClass + isNotSupported);
    }
  }

  @Override
  public Object resolve(Map<String, Object> context) {
    return expr.resolve(context);
  }

  @Override
  public Class<?> getType() {
    return target;
  }
}
