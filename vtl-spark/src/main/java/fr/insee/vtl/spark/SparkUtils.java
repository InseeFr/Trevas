package fr.insee.vtl.spark;

import static scala.collection.JavaConverters.iterableAsScalaIterable;

import java.util.Collection;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import scala.collection.Seq;

public final class SparkUtils {

  private SparkUtils() {}

  /**
   * Resolves a dataset column by logical name. Names containing {@code .} (e.g. VTL {@code
   * dataspace.dataset} or {@code total_work.taxis}) must be quoted for Spark, which otherwise
   * parses {@code a.b} as struct field {@code b} under {@code a}.
   */
  public static Column safeCol(String name) {
    if (name == null) {
      throw new IllegalArgumentException("Column name cannot be null");
    }
    return functions.col("`" + name.replace("`", "``") + "`");
  }

  public static Seq<Column> safeCols(Collection<String> names) {
    return iterableAsScalaIterable(names.stream().map(SparkUtils::safeCol).toList()).toSeq();
  }
}
