package fr.insee.vtl.engine.aggregation;

import fr.insee.vtl.model.DatasetExpression;
import java.util.ArrayList;
import java.util.List;

/** Result of resolving {@code group by} / {@code group all} for an aggregation. */
public final class GroupingPlan {

  private final DatasetExpression dataset;
  private final List<String> groupByKeys;

  public GroupingPlan(DatasetExpression dataset, List<String> groupByKeys) {
    this.dataset = dataset;
    this.groupByKeys = List.copyOf(groupByKeys);
  }

  public DatasetExpression dataset() {
    return dataset;
  }

  public List<String> groupByKeys() {
    return groupByKeys;
  }

  public static Builder builder(DatasetExpression initialDataset) {
    return new Builder(initialDataset);
  }

  public static final class Builder {
    private DatasetExpression dataset;
    private final List<String> groupByKeys = new ArrayList<>();

    private Builder(DatasetExpression dataset) {
      this.dataset = dataset;
    }

    public DatasetExpression getDataset() {
      return dataset;
    }

    public Builder withDataset(DatasetExpression dataset) {
      this.dataset = dataset;
      return this;
    }

    public Builder addGroupByKey(String key) {
      groupByKeys.add(key);
      return this;
    }

    public Builder addGroupByKeys(List<String> keys) {
      groupByKeys.addAll(keys);
      return this;
    }

    public GroupingPlan build() {
      return new GroupingPlan(dataset, groupByKeys);
    }
  }
}
