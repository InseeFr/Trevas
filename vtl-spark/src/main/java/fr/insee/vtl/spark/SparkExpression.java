package fr.insee.vtl.spark;

import fr.insee.vtl.model.ResolvableExpression;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext;
import org.apache.spark.sql.catalyst.expressions.codegen.ExprCode;
import org.apache.spark.sql.types.DataType;
import scala.collection.Seq;

import java.util.Objects;

public class SparkExpression extends Expression {

    private final ResolvableExpression expression;

    public SparkExpression(ResolvableExpression expression) {
        this.expression = Objects.requireNonNull(expression);
    }

    @Override
    public boolean nullable() {
        return false;
    }

    @Override
    public Object eval(InternalRow input) {

        return null;
    }

    @Override
    public ExprCode doGenCode(CodegenContext ctx, ExprCode ev) {
        return null;
    }

    @Override
    public DataType dataType() {
        return null;
    }

    @Override
    public Seq<Expression> children() {
        return null;
    }

    @Override
    public Object productElement(int n) {
        return null;
    }

    @Override
    public int productArity() {
        return 0;
    }

    @Override
    public boolean canEqual(Object that) {
        return false;
    }
}
