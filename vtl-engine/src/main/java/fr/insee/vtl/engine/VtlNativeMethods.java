package fr.insee.vtl.engine;

import com.github.hervian.reflection.Fun;
import fr.insee.vtl.engine.visitors.expression.ArithmeticExprOrConcatVisitor;
import fr.insee.vtl.engine.visitors.expression.ArithmeticVisitor;
import fr.insee.vtl.engine.visitors.expression.BooleanVisitor;
import fr.insee.vtl.engine.visitors.expression.ComparisonVisitor;
import fr.insee.vtl.engine.visitors.expression.UnaryVisitor;
import fr.insee.vtl.engine.visitors.expression.functions.ComparisonFunctionsVisitor;
import fr.insee.vtl.engine.visitors.expression.functions.DistanceFunctionsVisitor;
import fr.insee.vtl.engine.visitors.expression.functions.NumericFunctionsVisitor;
import fr.insee.vtl.engine.visitors.expression.functions.StringFunctionsVisitor;

import java.lang.reflect.Method;
import java.util.Set;

public class VtlNativeMethods {

    public static Set<Method> NATIVE_METHODS = Set.of(
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
            Fun.toMethod(ArithmeticExprOrConcatVisitor::addition),
            Fun.toMethod(ArithmeticExprOrConcatVisitor::subtraction),
            Fun.toMethod(ArithmeticExprOrConcatVisitor::concat),
            // ArithmeticVisitor
            Fun.toMethod(ArithmeticVisitor::multiplication),
            Fun.toMethod(ArithmeticVisitor::division),
            // DistanceFunctionsVisitor
            Fun.toMethod(DistanceFunctionsVisitor::levenshtein),
            // String function visitor
            Fun.toMethod(StringFunctionsVisitor::trim),
            Fun.toMethod(StringFunctionsVisitor::ltrim),
            Fun.toMethod(StringFunctionsVisitor::rtrim),
            Fun.toMethod(StringFunctionsVisitor::ucase),
            Fun.toMethod(StringFunctionsVisitor::lcase),
            Fun.toMethod(StringFunctionsVisitor::len),
            Fun.<String>toMethod(StringFunctionsVisitor::substr),
            Fun.<String, Long>toMethod(StringFunctionsVisitor::substr),
            Fun.<String, Long, Long>toMethod(StringFunctionsVisitor::substr),
            Fun.<String, String, String>toMethod(StringFunctionsVisitor::replace),
            Fun.<String, String>toMethod(StringFunctionsVisitor::replace),
            // ComparisonFunctionsVisitor
            Fun.toMethod(ComparisonFunctionsVisitor::between),
            Fun.toMethod(ComparisonFunctionsVisitor::charsetMatch),
            Fun.toMethod(ComparisonFunctionsVisitor::isNull),
            // BooleanVisitor
            Fun.toMethod(BooleanVisitor::and),
            Fun.toMethod(BooleanVisitor::or),
            Fun.toMethod(BooleanVisitor::xor),
            // UnaryVisitor
            Fun.toMethod(UnaryVisitor::plus),
            Fun.toMethod(UnaryVisitor::minus),
            Fun.toMethod(UnaryVisitor::not),
            // ComparisonVisitor
            Fun.<Comparable, Comparable>toMethod(ComparisonVisitor::isEqual),
            Fun.<Long, Double>toMethod(ComparisonVisitor::isEqual),
            Fun.<Double, Long>toMethod(ComparisonVisitor::isEqual),

            Fun.<Comparable, Comparable>toMethod(ComparisonVisitor::isNotEqual),
            Fun.<Long, Double>toMethod(ComparisonVisitor::isNotEqual),
            Fun.<Double, Long>toMethod(ComparisonVisitor::isNotEqual),

            Fun.<Comparable, Comparable>toMethod(ComparisonVisitor::isLessThan),
            Fun.<Long, Double>toMethod(ComparisonVisitor::isLessThan),
            Fun.<Double, Long>toMethod(ComparisonVisitor::isLessThan),
            Fun.<Comparable, Comparable>toMethod(ComparisonVisitor::isGreaterThan),
            Fun.<Long, Double>toMethod(ComparisonVisitor::isGreaterThan),
            Fun.<Double, Long>toMethod(ComparisonVisitor::isGreaterThan),
            Fun.<Comparable, Comparable>toMethod(ComparisonVisitor::isGreaterThanOrEqual),
            Fun.<Long, Double>toMethod(ComparisonVisitor::isGreaterThanOrEqual),
            Fun.<Double, Long>toMethod(ComparisonVisitor::isGreaterThanOrEqual),
            Fun.<Comparable, Comparable>toMethod(ComparisonVisitor::isLessThanOrEqual),
            Fun.<Long, Double>toMethod(ComparisonVisitor::isLessThanOrEqual),
            Fun.<Double, Long>toMethod(ComparisonVisitor::isLessThanOrEqual)
    );
}
