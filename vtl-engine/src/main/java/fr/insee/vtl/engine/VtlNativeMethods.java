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
            Fun.toMethod(UnaryVisitor::plus),
            Fun.toMethod(UnaryVisitor::minus),
            Fun.toMethod(UnaryVisitor::not),
            // ComparisonVisitor
            Fun.toMethod(ComparisonVisitor::isEqual),
            Fun.toMethod(ComparisonVisitor::isNotEqual),
            Fun.toMethod(ComparisonVisitor::isLessThan),
            Fun.toMethod(ComparisonVisitor::isGreaterThan),
            Fun.toMethod(ComparisonVisitor::isGreaterThanOrEqual),
            Fun.toMethod(ComparisonVisitor::isLessThanOrEqual)
    );
}
