package fr.insee.vtl.spark;

import static scala.collection.JavaConverters.iterableAsScalaIterable;

import java.util.Collection;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import scala.collection.Seq;

public final class SparkUtils {

  private SparkUtils() {}

  public static Column safeCol(String name) {
    return functions.col(quoteIdentifier(name));
  }

  public static String quoteIdentifier(String name) {
    if (name == null) {
      throw new IllegalArgumentException("Column name cannot be null");
    }
    return "`" + name.replace("`", "``") + "`";
  }

  public static Seq<Column> safeCols(Collection<String> names) {
    return iterableAsScalaIterable(names.stream().map(SparkUtils::safeCol).toList()).toSeq();
  }
}
