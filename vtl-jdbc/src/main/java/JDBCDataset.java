import fr.insee.vtl.model.Dataset;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class JDBCDataset implements Dataset {

    private final Supplier<ResultSet> resultSetSupplier;
    private DataStructure structure;

    public JDBCDataset(Supplier<ResultSet> resultSetSupplier) {
        this.resultSetSupplier = resultSetSupplier;
    }

    private static Map<String, Class<?>> getColumnTypes(ResultSetMetaData metaData) throws SQLException {
        var columnCount = metaData.getColumnCount();
        Map<String, Class<?>> columns = new LinkedHashMap<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            columns.put(
                    metaData.getColumnName(i),
                    toVtlType(metaData.getColumnType(i))
            );
        }
        return columns;
    }

    public static Class<?> toVtlType(Integer sqlType) {
        switch (sqlType) {
            case Types.BIGINT:
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
                return Long.class;
            case Types.DOUBLE:
            case Types.REAL:
            case Types.FLOAT:
            case Types.NUMERIC:
            case Types.DECIMAL:
                return Double.class;
            case Types.BOOLEAN:
            case Types.BIT:
                return Boolean.class;
            case Types.CHAR:
            case Types.CLOB:
            case Types.LONGNVARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.REF:
            case Types.SQLXML:
            case Types.STRUCT:
            case Types.VARCHAR:
                return String.class;
            default:
                throw new UnsupportedOperationException("unsuported type " + sqlType);
        }
    }

    /**
     * Comvert a resultSetMetaData to a Vtl Datastructure.
     * <p>
     * All the components are set to measures by default.
     */
    public static DataStructure toDataStructure(ResultSetMetaData metaData) throws SQLException {
        List<Component> components = new ArrayList<>();
        for (int columnIdx = 1; columnIdx <= metaData.getColumnCount(); columnIdx++) {
            var name = metaData.getColumnName(columnIdx);
            Class<?> type = toVtlType(metaData.getColumnType(columnIdx));
            components.add(new Component(name, type, Role.MEASURE));
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
            // TODO: Use vtl exception.
            throw new RuntimeException(se);
        }
    }

    private DataPoint toDataPoint(ResultSet resultSet) {
        try {
            DataPoint point = new DataPoint(getDataStructure());
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
            // TODO: Use vtl exception.
            throw new RuntimeException(se);
        }
    }

    @Override
    public DataStructure getDataStructure() {
        if (structure == null) {
            try (var resultSet = this.resultSetSupplier.get()) {
                structure = toDataStructure(resultSet.getMetaData());
            } catch (SQLException se) {
                // TODO: Use vtl exception.
                throw new RuntimeException(se);
            }
        }
        return structure;
    }
}