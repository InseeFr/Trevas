package fr.insee.vtl.engine;

import com.github.hervian.reflection.Fun;
import fr.insee.vtl.engine.visitors.expression.*;
import fr.insee.vtl.engine.visitors.expression.functions.ComparisonFunctionsVisitor;
import fr.insee.vtl.engine.visitors.expression.functions.DistanceFunctionsVisitor;
import fr.insee.vtl.engine.visitors.expression.functions.NumericFunctionsVisitor;
import fr.insee.vtl.engine.visitors.expression.functions.StringFunctionsVisitor;

import java.lang.reflect.Method;
import java.util.Set;

public class VtlNativeMethods {

    public static final Set<Method> NATIVE_METHODS = Set.of(
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
            Fun.toMethod(ComparisonVisitor::notIn)
    );

    private VtlNativeMethods() {
        throw new IllegalStateException("Utility class");
    }
}
