package fr.insee.vtl.spark;

import org.apache.spark.sql.Column;

import java.util.*;

import static org.apache.spark.sql.functions.*;

public class SparkAggrFuncExprBuilder {


    private static final int accuracy = 1000000;


    public static List<Column> getExpressions(Map<String, String> operations, Map<String, String> aliases) throws UnsupportedOperationException {
        List<Column> expressions = new ArrayList<>();
        // get keys
        Set<String> keys = aliases.keySet();
        // get the tail expression
        for (String key : keys) {
            expressions.add(buildExpression(aliases.get(key), operations.get(key), key));
        }
        return expressions;

    }

    private static Column buildExpression(String colName, String action, String aliasColName) throws UnsupportedOperationException {

        Column expression;
        switch (action.toLowerCase(Locale.ROOT)) {
            case "min":
                expression = min(colName).alias(aliasColName);
                break;
            case "max":
                expression = max(colName).alias(aliasColName);
                break;
            case "avg":
                expression = avg(colName).alias(aliasColName);
                break;
            case "sum":
                expression = sum(colName).alias(aliasColName);
                break;
            case "count":
                expression = count("*").alias(aliasColName);
                break;
            case "median":
                expression = percentile_approx(col(colName), lit(0.5), lit(accuracy)).alias(aliasColName);
                break;
            case "stddev_pop":
                expression = stddev_pop(colName).alias(aliasColName);
                break;
            case "stddev_samp":
                expression = stddev_samp(colName).alias(aliasColName);
                break;
            case "var_pop":
                expression = var_pop(colName).alias(aliasColName);
                break;
            case "var_samp":
                expression = var_samp(colName).alias(aliasColName);
                break;
            case "collect_list":
                expression = collect_list(colName).alias(aliasColName);
                break;
            default:
                throw new UnsupportedOperationException("unknown aggregation action: "+action);
        }
        return expression;
    }

}
