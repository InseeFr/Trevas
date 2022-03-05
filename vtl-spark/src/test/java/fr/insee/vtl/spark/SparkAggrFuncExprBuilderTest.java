package fr.insee.vtl.spark;

import fr.insee.vtl.model.ProcessingEngineFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

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
       // operations.get("age").add("median");
        operations.put("weight", weightFunctions);
        operations.get("weight").add("max");
       // operations.get("weight").add("median");
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
       // aliases.get("age").add("medianAge");
        aliases.put("weight", weightAliasCols);
        aliases.get("weight").add("maxWeight");
       // aliases.get("weight").add("medianWeight");
        aliases.put("null", nullAliasCols);
        aliases.get("null").add("countVal");
    }

    @Test
    public void testGetExpressions() throws Exception {
        SparkAggrFuncExprBuilder builder=new SparkAggrFuncExprBuilder(operations,aliases);
        builder.getHeadExpression();
        builder.getTailExpressions();
    }

}
