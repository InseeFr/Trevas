package fr.insee.vtl.jdbc;

import fr.insee.vtl.model.Dataset;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/** The <code>JDBCDataset</code> class is a wrapper around a SQL result set. */
public class JDBCDataset implements Dataset {

  private final Supplier<ResultSet> resultSetSupplier;
  private DataStructure structure;

  /**
   * Creates a new JDBC Dataset.
   *
   * <p>The given supplier is called to create a new result set whenever the data or the data
   * structure is requested.
   */
  public JDBCDataset(Supplier<ResultSet> resultSetSupplier) {
    this.resultSetSupplier = resultSetSupplier;
  }

  /**
   * Translates a SQL data type into a VTL data type.
   *
   * @param sqlType the SQL data type to translate (as an integer).
   * @return The corresponding VTL data type as a class.
   */
  public static Class<?> toVtlType(Integer sqlType) {
    return switch (sqlType) {
      case Types.BIGINT, Types.INTEGER, Types.SMALLINT, Types.TINYINT -> Long.class;
      case Types.DOUBLE, Types.REAL, Types.FLOAT, Types.NUMERIC, Types.DECIMAL -> Double.class;
      case Types.BOOLEAN, Types.BIT -> Boolean.class;
      case Types.CHAR,
          Types.CLOB,
          Types.LONGNVARCHAR,
          Types.LONGVARCHAR,
          Types.NVARCHAR,
          Types.REF,
          Types.SQLXML,
          Types.STRUCT,
          Types.VARCHAR ->
          String.class;
      default -> throw new UnsupportedOperationException("unsupported type " + sqlType);
    };
  }

  /**
   * Converts a {@link ResultSetMetaData} to a VTL data structure.
   *
   * <p>All the components are considered measures by default.
   */
  public static DataStructure toDataStructure(ResultSetMetaData metaData) throws SQLException {
    List<Component> components = new ArrayList<>();
    for (int columnIdx = 1; columnIdx <= metaData.getColumnCount(); columnIdx++) {
      var name = metaData.getColumnName(columnIdx);
      Class<?> type = toVtlType(metaData.getColumnType(columnIdx));
      // TODO: refine nullable strategy
      components.add(new Component(name, type, Role.MEASURE, true));
    }
    return new DataStructure(components);
  }

  @Override
  public List<DataPoint> getDataPoints() {
    try (var resultSet = this.resultSetSupplier.get()) {
      var result = new ArrayList<DataPoint>();
      while (resultSet.next()) {
        result.add(toDataPoint(resultSet));
      }
      return result;
    } catch (SQLException se) {
      throw new RuntimeException(se);
    }
  }

  private DataPoint toDataPoint(ResultSet resultSet) {
    try {
      DataPoint point = new DataPoint(getDataStructure(resultSet));
      for (String column : structure.keySet()) {
        if (String.class.equals(structure.get(column).getType())) {
          point.set(column, resultSet.getString(column));
        } else if (Long.class.equals(structure.get(column).getType())) {
          point.set(column, resultSet.getLong(column));
        } else if (Double.class.equals(structure.get(column).getType())) {
          point.set(column, resultSet.getDouble(column));
        } else if (Boolean.class.equals(structure.get(column).getType())) {
          point.set(column, resultSet.getBoolean(column));
        } else {
          throw new IllegalStateException("Unexpected value: " + structure.get(column).getType());
        }
      }
      return point;
    } catch (SQLException se) {
      throw new RuntimeException(se);
    }
  }

  private DataStructure getDataStructure(ResultSet resultSet) throws SQLException {
    if (structure == null) {
      structure = toDataStructure(resultSet.getMetaData());
    }
    return structure;
  }

  @Override
  public DataStructure getDataStructure() {
    try (var resultSet = this.resultSetSupplier.get()) {
      return getDataStructure(resultSet);
    } catch (SQLException se) {
      throw new RuntimeException(se);
    }
  }
}
