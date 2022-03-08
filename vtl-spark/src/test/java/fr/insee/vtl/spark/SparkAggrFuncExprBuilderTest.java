package fr.insee.vtl.spark;


import org.apache.spark.sql.Column;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;


import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SparkAggrFuncExprBuilderTest {

    private List<AbstractMap.SimpleImmutableEntry<String, String>> operations;
    private List<AbstractMap.SimpleImmutableEntry<String, String>> aliases;

    @BeforeEach
    public void setup() {
        operations = new ArrayList<>();

        AbstractMap.SimpleImmutableEntry<String, String> entry1 = new AbstractMap.SimpleImmutableEntry<String, String>("age", "sum");
        AbstractMap.SimpleImmutableEntry<String, String> entry2 = new AbstractMap.SimpleImmutableEntry<String, String>("weight", "avg");
        AbstractMap.SimpleImmutableEntry<String, String> entry3 = new AbstractMap.SimpleImmutableEntry<String, String>("null", "count");
        AbstractMap.SimpleImmutableEntry<String, String> entry4 = new AbstractMap.SimpleImmutableEntry<String, String>("age", "max");
        AbstractMap.SimpleImmutableEntry<String, String> entry5 = new AbstractMap.SimpleImmutableEntry<String, String>("weight", "max");
        AbstractMap.SimpleImmutableEntry<String, String> entry6 = new AbstractMap.SimpleImmutableEntry<String, String>("age", "min");
        AbstractMap.SimpleImmutableEntry<String, String> entry7 = new AbstractMap.SimpleImmutableEntry<String, String>("weight", "min");
        AbstractMap.SimpleImmutableEntry<String, String> entry8 = new AbstractMap.SimpleImmutableEntry<String, String>("age", "median");
        AbstractMap.SimpleImmutableEntry<String, String> entry9 = new AbstractMap.SimpleImmutableEntry<String, String>("weight", "median");
        operations.add(entry1);
        operations.add(entry2);
        operations.add(entry3);
        operations.add(entry4);
        operations.add(entry5);
        operations.add(entry6);
        operations.add(entry7);
        operations.add(entry8);
        operations.add(entry9);

        aliases = new ArrayList<>();

        AbstractMap.SimpleImmutableEntry<String, String> a1 = new AbstractMap.SimpleImmutableEntry<String, String>("age", "sumAge");
        AbstractMap.SimpleImmutableEntry<String, String> a2 = new AbstractMap.SimpleImmutableEntry<String, String>("weight", "avgWeight");
        AbstractMap.SimpleImmutableEntry<String, String> a3 = new AbstractMap.SimpleImmutableEntry<String, String>("null", "countVal");
        AbstractMap.SimpleImmutableEntry<String, String> a4 = new AbstractMap.SimpleImmutableEntry<String, String>("age", "maxAge");
        AbstractMap.SimpleImmutableEntry<String, String> a5 = new AbstractMap.SimpleImmutableEntry<String, String>("weight", "maxWeight");
        AbstractMap.SimpleImmutableEntry<String, String> a6 = new AbstractMap.SimpleImmutableEntry<String, String>("age", "minAge");
        AbstractMap.SimpleImmutableEntry<String, String> a7 = new AbstractMap.SimpleImmutableEntry<String, String>("weight", "minWeight");
        AbstractMap.SimpleImmutableEntry<String, String> a8 = new AbstractMap.SimpleImmutableEntry<String, String>("age", "medianAge");
        AbstractMap.SimpleImmutableEntry<String, String> a9 = new AbstractMap.SimpleImmutableEntry<String, String>("weight", "medianWeight");

        aliases.add(a1);
        aliases.add(a2);
        aliases.add(a3);
        aliases.add(a4);
        aliases.add(a5);
        aliases.add(a6);
        aliases.add(a7);
        aliases.add(a8);
        aliases.add(a9);

    }

    @Test
    public void testGetExpressions() throws Exception {

        String expectedHeadExpression="sum(age) AS sumAge";
        List<String> expectedExpression = new ArrayList<>();
        expectedExpression.add("avg(weight) AS avgWeight");
        expectedExpression.add("count(1) AS countVal");
        expectedExpression.add("max(age) AS maxAge");
        expectedExpression.add("max(weight) AS maxWeight");
        expectedExpression.add("min(age) AS minAge");
        expectedExpression.add("min(weight) AS minWeight");
        expectedExpression.add("percentile_approx(age, 0.5, 1000000) AS medianAge");
        expectedExpression.add("percentile_approx(weight, 0.5, 1000000) AS medianWeight");


        SparkAggrFuncExprBuilder builder = new SparkAggrFuncExprBuilder(operations, aliases);
        String headExpression = builder.getHeadExpression().toString();
        List<Column> tailExpressions = builder.getTailExpressions();
        //compare head
        assertEquals(headExpression, expectedHeadExpression);

        //compare tail
        for (int i=0;i<tailExpressions.size();i++) {

            assertThat(tailExpressions.get(i).toString().equals(expectedExpression.get(i)));
        }
    }

}
