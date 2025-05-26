package fr.insee.vtl.engine;

import com.github.hervian.reflection.Fun;
import fr.insee.vtl.engine.visitors.expression.*;
import fr.insee.vtl.engine.visitors.expression.functions.ComparisonFunctionsVisitor;
import fr.insee.vtl.engine.visitors.expression.functions.DistanceFunctionsVisitor;
import fr.insee.vtl.engine.visitors.expression.functions.NumericFunctionsVisitor;
import fr.insee.vtl.engine.visitors.expression.functions.StringFunctionsVisitor;
import java.lang.reflect.Method;
import java.time.*;
import java.util.Set;
import org.threeten.extra.Interval;
import org.threeten.extra.PeriodDuration;

public class VtlNativeMethods {

  public static final Set<Method> NATIVE_METHODS =
      Set.of(
          // NumericFunctionsVisitor
          Fun.toMethod(NumericFunctionsVisitor::ceil),
          Fun.toMethod(NumericFunctionsVisitor::floor),
          Fun.toMethod(NumericFunctionsVisitor::abs),
          Fun.toMethod(NumericFunctionsVisitor::exp),
          Fun.toMethod(NumericFunctionsVisitor::ln),
          Fun.toMethod(NumericFunctionsVisitor::sqrt),
          Fun.toMethod(NumericFunctionsVisitor::round),
          Fun.toMethod(NumericFunctionsVisitor::trunc),
          Fun.toMethod(NumericFunctionsVisitor::mod),
          Fun.toMethod(NumericFunctionsVisitor::power),
          Fun.toMethod(NumericFunctionsVisitor::random),
          Fun.toMethod(NumericFunctionsVisitor::log),
          // ArithmeticExprOrConcatVisitor
          Fun.<Long, Long>toMethod(ArithmeticExprOrConcatVisitor::addition),
          Fun.<Double, Long>toMethod(ArithmeticExprOrConcatVisitor::addition),
          Fun.<Long, Double>toMethod(ArithmeticExprOrConcatVisitor::addition),
          Fun.<Double, Double>toMethod(ArithmeticExprOrConcatVisitor::addition),
          Fun.<Long, Long>toMethod(ArithmeticExprOrConcatVisitor::subtraction),
          Fun.<Double, Long>toMethod(ArithmeticExprOrConcatVisitor::subtraction),
          Fun.<Long, Double>toMethod(ArithmeticExprOrConcatVisitor::subtraction),
          Fun.<Double, Double>toMethod(ArithmeticExprOrConcatVisitor::subtraction),
          Fun.toMethod(ArithmeticExprOrConcatVisitor::concat),
          // Conditional
          Fun.<Boolean, Long, Long>toMethod(ConditionalVisitor::ifThenElse),
          Fun.<Boolean, Double, Double>toMethod(ConditionalVisitor::ifThenElse),
          Fun.<Boolean, String, String>toMethod(ConditionalVisitor::ifThenElse),
          Fun.<Boolean, Boolean, Boolean>toMethod(ConditionalVisitor::ifThenElse),
          Fun.<Long, Long>toMethod(ConditionalVisitor::nvl),
          Fun.<Double, Double>toMethod(ConditionalVisitor::nvl),
          Fun.<Double, Long>toMethod(ConditionalVisitor::nvl),
          Fun.<Long, Double>toMethod(ConditionalVisitor::nvl),
          Fun.<String, String>toMethod(ConditionalVisitor::nvl),
          Fun.<Boolean, Boolean>toMethod(ConditionalVisitor::nvl),
          // ArithmeticVisitor
          Fun.<Long, Long>toMethod(ArithmeticVisitor::multiplication),
          Fun.<Double, Long>toMethod(ArithmeticVisitor::multiplication),
          Fun.<Long, Double>toMethod(ArithmeticVisitor::multiplication),
          Fun.<Double, Double>toMethod(ArithmeticVisitor::multiplication),
          Fun.<Long, Long>toMethod(ArithmeticVisitor::division),
          Fun.<Double, Long>toMethod(ArithmeticVisitor::division),
          Fun.<Long, Double>toMethod(ArithmeticVisitor::division),
          Fun.<Double, Double>toMethod(ArithmeticVisitor::division),
          // DistanceFunctionsVisitor
          Fun.toMethod(DistanceFunctionsVisitor::levenshtein),
          // String function visitor
          Fun.toMethod(StringFunctionsVisitor::trim),
          Fun.toMethod(StringFunctionsVisitor::ltrim),
          Fun.toMethod(StringFunctionsVisitor::rtrim),
          Fun.toMethod(StringFunctionsVisitor::ucase),
          Fun.toMethod(StringFunctionsVisitor::lcase),
          Fun.toMethod(StringFunctionsVisitor::len),
          Fun.toMethod(StringFunctionsVisitor::substr),
          Fun.toMethod(StringFunctionsVisitor::replace),
          Fun.toMethod(StringFunctionsVisitor::instr),
          // ComparisonFunctionsVisitor
          Fun.toMethod(ComparisonFunctionsVisitor::between),
          Fun.toMethod(ComparisonFunctionsVisitor::charsetMatch),
          Fun.toMethod(ComparisonFunctionsVisitor::isNull),
          // BooleanVisitor
          Fun.toMethod(BooleanVisitor::and),
          Fun.toMethod(BooleanVisitor::or),
          Fun.toMethod(BooleanVisitor::xor),
          // UnaryVisitor
          Fun.<Long>toMethod(UnaryVisitor::plus),
          Fun.<Double>toMethod(UnaryVisitor::plus),
          Fun.<Long>toMethod(UnaryVisitor::minus),
          Fun.<Double>toMethod(UnaryVisitor::minus),
          Fun.toMethod(UnaryVisitor::not),
          // ComparisonVisitor
          Fun.toMethod(ComparisonVisitor::isEqual),
          Fun.toMethod(ComparisonVisitor::isNotEqual),
          Fun.toMethod(ComparisonVisitor::isLessThan),
          Fun.toMethod(ComparisonVisitor::isGreaterThan),
          Fun.toMethod(ComparisonVisitor::isGreaterThanOrEqual),
          Fun.toMethod(ComparisonVisitor::isLessThanOrEqual),
          Fun.toMethod(ComparisonVisitor::in),
          Fun.toMethod(ComparisonVisitor::notIn),

          // Temporal functions
          Fun.<Instant, PeriodDuration>toMethod(TemporalFunctions::addition),
          Fun.<ZonedDateTime, PeriodDuration>toMethod(TemporalFunctions::addition),
          Fun.<OffsetDateTime, PeriodDuration>toMethod(TemporalFunctions::addition),
          Fun.<PeriodDuration, Instant>toMethod(TemporalFunctions::addition),
          Fun.<PeriodDuration, ZonedDateTime>toMethod(TemporalFunctions::addition),
          Fun.<PeriodDuration, OffsetDateTime>toMethod(TemporalFunctions::addition),
          Fun.<Instant, PeriodDuration>toMethod(TemporalFunctions::subtraction),
          Fun.<ZonedDateTime, PeriodDuration>toMethod(TemporalFunctions::subtraction),
          Fun.<OffsetDateTime, PeriodDuration>toMethod(TemporalFunctions::subtraction),
          Fun.<PeriodDuration, Instant>toMethod(TemporalFunctions::subtraction),
          Fun.<PeriodDuration, ZonedDateTime>toMethod(TemporalFunctions::subtraction),
          Fun.<PeriodDuration, OffsetDateTime>toMethod(TemporalFunctions::subtraction),
          Fun.<Instant, Instant>toMethod(TemporalFunctions::subtraction),
          Fun.<Instant, ZonedDateTime>toMethod(TemporalFunctions::subtraction),
          Fun.<Instant, OffsetDateTime>toMethod(TemporalFunctions::subtraction),
          Fun.<ZonedDateTime, Instant>toMethod(TemporalFunctions::subtraction),
          Fun.<ZonedDateTime, ZonedDateTime>toMethod(TemporalFunctions::subtraction),
          Fun.<ZonedDateTime, OffsetDateTime>toMethod(TemporalFunctions::subtraction),
          Fun.<OffsetDateTime, Instant>toMethod(TemporalFunctions::subtraction),
          Fun.<OffsetDateTime, ZonedDateTime>toMethod(TemporalFunctions::subtraction),
          Fun.<OffsetDateTime, OffsetDateTime>toMethod(TemporalFunctions::subtraction),
          Fun.<PeriodDuration, Long>toMethod(TemporalFunctions::multiplication),
          Fun.<Long, PeriodDuration>toMethod(TemporalFunctions::multiplication),
          Fun.toMethod(TemporalFunctions::timeshift),
          Fun.<Instant, String, String>toMethod(TemporalFunctions::truncate_time),
          Fun.<Instant, String>toMethod(TemporalFunctions::truncate_time),
          Fun.<ZonedDateTime, String>toMethod(TemporalFunctions::truncate_time),
          Fun.<OffsetDateTime, String>toMethod(TemporalFunctions::truncate_time),
          Fun.<Interval, String>toMethod(TemporalFunctions::truncate_time),
          Fun.<Interval, String, String>toMethod(TemporalFunctions::truncate_time),
          Fun.toMethod(TemporalFunctions::at_zone));

  private VtlNativeMethods() {
    throw new IllegalStateException("Utility class");
  }
}
