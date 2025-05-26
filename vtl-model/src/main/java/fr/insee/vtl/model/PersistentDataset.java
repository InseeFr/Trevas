package fr.insee.vtl.model;

import java.util.List;
import java.util.Objects;

public final class PersistentDataset implements Dataset {

  private final Dataset delegate;

  public PersistentDataset(Dataset t) {
    this.delegate = Objects.requireNonNull(t);
  }

  public Dataset getDelegate() {
    return this.delegate;
  }

  @Override
  public List<DataPoint> getDataPoints() {
    return delegate.getDataPoints();
  }

  @Override
  public DataStructure getDataStructure() {
    return delegate.getDataStructure();
  }
}
