package fr.insee.vtl.spark;

import fr.insee.vtl.model.ResolvableExpression;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

/**
 * The <code>SparkFilterFunction</code> class is a wrapper around a filter expression operating on rows of a Spark dataset.
 */
public class SparkFilterFunction implements FilterFunction<Row> {

    private final ResolvableExpression expression;

    /**
     * Constructor taking a VTL expression.
     *
     * @param expression the VTL expression.
     */
    public SparkFilterFunction(ResolvableExpression expression) {
        this.expression = expression;
    }

    @Override
    public boolean call(Row row) {
        var res = expression.resolve(new SparkRowMap(row));
        if (res == null) return false;
        return (boolean) res;
    }
}
