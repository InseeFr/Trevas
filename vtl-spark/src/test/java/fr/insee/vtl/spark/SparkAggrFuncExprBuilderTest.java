package fr.insee.vtl.spark;


import org.apache.spark.sql.Column;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class SparkAggrFuncExprBuilderTest {

    private Map<String, String> operations;
    private Map<String, String> aliases;

    @BeforeEach
    public void setup() {


        operations = new LinkedHashMap<>();

        operations.put("sumAge", "sum");
        operations.put("avgWeight", "avg");
        operations.put("countVal", "count");
        operations.put("maxAge", "max");
        operations.put("maxWeight", "max");
        operations.put("minAge", "min");
        operations.put("minWeight", "min");
        operations.put("medianAge", "median");
        operations.put("medianWeight", "median");
        operations.put("stdPopAge", "stddev_pop");
        operations.put("stdSampAge", "stddev_samp");
        operations.put("varPopWeight", "var_pop");
        operations.put("varSampWeight", "var_samp");


        aliases = new LinkedHashMap<>();

        aliases.put("sumAge", "age");
        aliases.put("avgWeight", "weight");
        aliases.put("countVal", "null");
        aliases.put("maxAge", "age");
        aliases.put("maxWeight", "weight");
        aliases.put("minAge", "age");
        aliases.put("minWeight", "weight");
        aliases.put("medianAge", "age");
        aliases.put("medianWeight", "weight");
        aliases.put("stdPopAge", "age");
        aliases.put("stdSampAge", "age");
        aliases.put("varPopWeight", "weight");
        aliases.put("varSampWeight", "weight");
    }

    @Test
    public void testGetExpressions() throws Exception {

        List<String> expectedExpression = new ArrayList<>();
        expectedExpression.add("sum(age) AS sumAge");
        expectedExpression.add("avg(weight) AS avgWeight");
        expectedExpression.add("count(1) AS countVal");
        expectedExpression.add("max(age) AS maxAge");
        expectedExpression.add("max(weight) AS maxWeight");
        expectedExpression.add("min(age) AS minAge");
        expectedExpression.add("min(weight) AS minWeight");
        expectedExpression.add("percentile_approx(age, 0.5, 1000000) AS medianAge");
        expectedExpression.add("percentile_approx(weight, 0.5, 1000000) AS medianWeight");
        expectedExpression.add("stddev_pop(age) AS stdPopAge");
        expectedExpression.add("stddev_samp(age) AS stdSampAge");
        expectedExpression.add("var_pop(weight) AS varPopWeight");
        expectedExpression.add("var_samp(weight) AS varSampWeight");


        List<Column> expressions = SparkAggrFuncExprBuilder.getExpressions(operations, aliases);

        //compare expression list
        for (int i = 0; i < expressions.size(); i++) {
            assertThat(expressions.get(i).toString().equals(expectedExpression.get(i)));
        }
    }

}
