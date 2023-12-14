package fr.insee.vtl.spark.processing.engine;

import fr.insee.vtl.model.Dataset;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;

public class TestUtilities {

    public static List<Map<String, Object>> roundDecimalInDataset(Dataset dataset){
        List<Map<String, Object>> res = dataset.getDataAsMap();
        for (Map<String, Object> map : res) {
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                if (entry.getValue() instanceof Double || entry.getValue() instanceof Float) {
                    double value = ((Number) entry.getValue()).doubleValue();
                    BigDecimal roundedValue = BigDecimal.valueOf(value).setScale(2, RoundingMode.HALF_UP);
                    map.put(entry.getKey(), roundedValue.doubleValue());
                }
            }
        }
        return res;
    }

}
