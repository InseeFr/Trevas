package fr.insee.vtl.engine.utils;

import fr.insee.vtl.model.ResolvableExpression;

import java.math.BigDecimal;

import static fr.insee.vtl.engine.utils.TypeChecking.isDouble;
import static fr.insee.vtl.engine.utils.TypeChecking.isLong;

public class NumberConvertors {

    private NumberConvertors() {
        throw new IllegalStateException("Utility class");
    }

    public static BigDecimal asBigDecimal(ResolvableExpression expr, Object resolved) {
        if (resolved == null) return null;
        if (isLong(expr)) return BigDecimal.valueOf(Double.valueOf((Long) resolved));
        if (isDouble(expr)) return BigDecimal.valueOf((Double) resolved);
        return null;
    }

}
