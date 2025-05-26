package fr.insee.vtl.model;

import java.util.Map;

/**
 * The <code>DatasetExpression</code> class is an abstract representation of a dataset expression.
 */
public abstract class DatasetExpression extends ResolvableExpression implements Structured {

  public DatasetExpression(Positioned position) {
    super(position);
  }

  /**
   * Returns a dataset expression based on a given dataset.
   *
   * @param value The dataset on which the expression should be based.
   * @return The dataset expression.
   */
  public static DatasetExpression of(Dataset value, Positioned position) {
    return new DatasetExpression(position) {

      @Override
      public Structured.DataStructure getDataStructure() {
        return value.getDataStructure();
      }

      @Override
      public Dataset resolve(Map<String, Object> na) {
        return value;
      }
    };
  }

  @Override
  public abstract Dataset resolve(Map<String, Object> context);

  @Override
  public Class<?> getType() {
    return Dataset.class;
  }
}
