package fr.insee.vtl.spark;

import org.apache.spark.sql.Column;

import java.util.*;

import static org.apache.spark.sql.functions.*;

public class SparkAggrFuncExprBuilder {

    private final List<AbstractMap.SimpleImmutableEntry<String, String>> operations;
    private final List<AbstractMap.SimpleImmutableEntry<String, String>> aliases;
    private List<Column> tailExpressions;
    private Column headExpression;
    private final int accuracy = 1000000;

    public SparkAggrFuncExprBuilder(List<AbstractMap.SimpleImmutableEntry<String, String>> operations,
                                    List<AbstractMap.SimpleImmutableEntry<String, String>> aliases) throws Exception {
        this.operations = operations;
        this.aliases = aliases;
        this.parseExpressions();
    }

    public List<Column> getTailExpressions() {
        return tailExpressions;
    }

    public Column getHeadExpression() {
        return headExpression;
    }

    private void parseExpressions() throws Exception {
        tailExpressions = new ArrayList<>();
        // get head expression
        headExpression = buildExpression(operations.get(0).getKey(), operations.get(0).getValue(), aliases.get(0).getValue());

        // get the tail expression
        int index = 1;
        while (index < operations.size()) {
            tailExpressions.add(buildExpression(operations.get(index).getKey(), operations.get(index).getValue(), aliases.get(index).getValue()));
            index++;
        }


    }

    private Column buildExpression(String colName, String action, String aliasColName) throws Exception {

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
                throw new Exception("Unknown aggregation action");
        }
        return expression;
    }

}
