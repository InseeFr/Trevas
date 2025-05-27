package fr.insee.vtl.model;

public class Analytics {

  public enum Order {
    ASC,
    DESC
  }

  public enum Function {
    SUM,
    AVG,
    COUNT,
    MEDIAN,
    MIN,
    MAX,
    STDDEV_POP,
    STDDEV_SAMP,
    VAR_POP,
    VAR_SAMP,
    FIRST_VALUE,
    LAST_VALUE,
    LEAD,
    LAG,
    RATIO_TO_REPORT,
    RANK,
  }

  public abstract static class WindowSpec {
    private final Long lower;
    private final Long upper;

    WindowSpec(Long lower, Long upper) {
      this.lower = lower;
      this.upper = upper;
    }

    public Long getUpper() {
      return upper;
    }

    public Long getLower() {
      return lower;
    }
  }

  public static class RangeWindow extends WindowSpec {

    public RangeWindow(Long lower, Long upper) {
      super(lower, upper);
    }
  }

  public static class DataPointWindow extends WindowSpec {
    public DataPointWindow(Long lower, Long upper) {
      super(lower, upper);
    }
  }
}
