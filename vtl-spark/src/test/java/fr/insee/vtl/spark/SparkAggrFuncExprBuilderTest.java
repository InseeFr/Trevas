package fr.insee.vtl.spark;


import org.apache.spark.sql.Column;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;


import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SparkAggrFuncExprBuilderTest {

    private Map<String, List<String>> operations;
    private Map<String, List<String>> aliases;

    @BeforeEach
    public void setup() {
        operations = new LinkedHashMap<>();
        List ageColFunctions = new ArrayList();
        List weightFunctions = new ArrayList();
        List nullFunctions = new ArrayList();

        operations.put("age", ageColFunctions);
        operations.get("age").add("sum");
        operations.get("age").add("max");
        operations.get("age").add("min");
        operations.get("age").add("median");
        operations.put("weight", weightFunctions);
        operations.get("weight").add("max");
        operations.get("weight").add("median");
        operations.put("null", nullFunctions);
        operations.get("null").add("count");

        aliases = new LinkedHashMap<>();
        List ageAliasCols = new ArrayList();
        List weightAliasCols = new ArrayList();
        List nullAliasCols = new ArrayList();

        aliases.put("age", ageAliasCols);
        aliases.get("age").add("sumAge");
        aliases.get("age").add("maxAge");
        aliases.get("age").add("minAge");
        aliases.get("age").add("medianAge");
        aliases.put("weight", weightAliasCols);
        aliases.get("weight").add("maxWeight");
        aliases.get("weight").add("medianWeight");
        aliases.put("null", nullAliasCols);
        aliases.get("null").add("countVal");
    }

    @Test
    public void testGetExpressions() throws Exception {

        String expectedHeadExpression="sum(age) AS sumAge";
        List<String> expectedExpression = new ArrayList<>();
        expectedExpression.add("max(age) AS maxAge");
        expectedExpression.add("min(age) AS minAge");
        expectedExpression.add("percentile_approx(age, 0.5, 1000000) AS medianAge");
        expectedExpression.add("max(weight) AS maxWeight");
        expectedExpression.add("percentile_approx(weight, 0.5, 1000000) AS medianWeight");
        expectedExpression.add("count(1) AS countVal");


        SparkAggrFuncExprBuilder builder = new SparkAggrFuncExprBuilder(operations, aliases);
        String headExpression = builder.getHeadExpression().toString();
        List<Column> tailExpressions = builder.getTailExpressions();
        //compare head
        assertEquals(headExpression, expectedHeadExpression);

        //compare tail
        for (int i=0;i<tailExpressions.size();i++) {
            assertThat(tailExpressions.get(i).equals(expectedExpression.get(i)));
        }
    }

}
