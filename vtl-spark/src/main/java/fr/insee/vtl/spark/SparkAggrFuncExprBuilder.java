package fr.insee.vtl.spark;

import org.apache.spark.sql.Column;

import java.util.*;

import static org.apache.spark.sql.functions.*;

public class SparkAggrFuncExprBuilder {
    private final Map<String, List<String>> operations;
    private final Map<String, List<String>> aliases;
    private List<Column> tailExpressions;
    private Column headExpression;
    private final int accuracy=1000000;

    public SparkAggrFuncExprBuilder(Map<String, List<String>> operations, Map<String,List<String>> aliases) throws Exception {
          this.operations=operations;
          this.aliases=aliases;
          this.parseExpressions();
    }

    public List<Column> getTailExpressions() {
        return tailExpressions;
    }

    public Column getHeadExpression() {
        return headExpression;
    }

    private void parseExpressions() throws Exception {
        tailExpressions=new ArrayList<>();
        String headKey="";
        Iterator<String> iterator = this.operations.keySet().iterator();
        // get the head(first) expression
        if (iterator.hasNext()) {
            headKey = iterator.next();
            // get head expression
            headExpression=buildExpression(headKey,operations.get(headKey).get(0),aliases.get(headKey).get(0));
        }

        iterator=this.operations.keySet().iterator();
        // get the tail expression
        while (iterator.hasNext()) {
            String key = iterator.next();
            // if the key is headKey, skip the first action
            List<String> functions = operations.get(key);
            if (key.equals(headKey)){
                if (functions.size()>1){
                    for (int i=1;i<functions.size();i++){
                        tailExpressions.add(buildExpression(headKey,operations.get(headKey).get(i),aliases.get(headKey).get(i)));
                    }
                }
            }
            else {
                for (int i=0;i<functions.size();i++){
                    tailExpressions.add(buildExpression(key,operations.get(key).get(i),aliases.get(key).get(i)));
                }
            }

        }
    }

    private Column buildExpression(String colName,String action,String aliasColName) throws Exception {

        Column expression;
        switch (action.toLowerCase(Locale.ROOT)) {
            case "min":
                expression= min(colName).alias(aliasColName);
                break;
            case "max":
                expression= max(colName).alias(aliasColName);
                break;
            case "avg":
                expression= avg(colName).alias(aliasColName);
                break;
            case "sum":
                expression= sum(colName).alias(aliasColName);
                break;
            case "count":
                expression= count("*").alias(aliasColName);
                break;
            case "median":
                expression=  percentile_approx(col(colName), lit(0.5), lit(accuracy)).alias(aliasColName);
                break;
            case "stddev_pop":
                expression= stddev_pop(colName).alias(aliasColName);
                break;
            case "stddev_samp":
                expression= stddev_samp(colName).alias(aliasColName);
                break;
            case "var_pop":
                expression= var_pop(colName).alias(aliasColName);
                break;
            case "var_samp":
                expression= var_samp(colName).alias(aliasColName);
                break;
            case "collect_list":
                expression= collect_list(colName).alias(aliasColName);
                break;
            default:
                throw new Exception("Unknown aggregation action");
        }
        return expression;
    }

}
