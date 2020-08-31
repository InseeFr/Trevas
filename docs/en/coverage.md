# VTL tools - Engine coverage

## Java interpretor

### Functional coverage

#### VTL-ML - General purpose operators

| Name                              | Symbol |     Supported      |
| --------------------------------- | :----- | :----------------: |
| Parentheses                       | ( )    | :heavy_check_mark: |
| Persistent assignment             | <-     |        :x:         |
| Temporary assignment              | :=     | :heavy_check_mark: |
| Membership                        | #      |        :x:         |
| User-defined operator call        |        |        :x:         |
| Evaluation of an external routine | eval   |        :x:         |
| Type conversion                   | cast   |        :x:         |

#### VTL-ML - Join operators

| Name | Symbol                                       | Supported |
| ---- | :------------------------------------------- | :-------: |
| Join | inner_join, left_join, full_join, cross_join |    :x:    |

#### VTL-ML - String operators

| Name                       | Symbol             |     Supported      |
| -------------------------- | :----------------- | :----------------: |
| String concatenation       | &#124;&#124;       | :heavy_check_mark: |
| Whitespace removal         | trim, rtrim, ltrim | :heavy_check_mark: |
| Character case conversion  | upper/lower        | :heavy_check_mark: |
| Sub-string extraction      | substr             | :heavy_check_mark: |
| String pattern replacement | replace            | :heavy_check_mark: |
| String pattern location    | instr              | :heavy_check_mark: |
| String length              | length             | :heavy_check_mark: |

#### VTL-ML - Numeric operators

| Name              | Symbol       |     Supported      |
| ----------------- | :----------- | :----------------: |
| Unary plus        | +            | :heavy_check_mark: |
| Unary minus       | -            | :heavy_check_mark: |
| Addition          | +            | :heavy_check_mark: |
| Subtraction       | -            | :heavy_check_mark: |
| Multiplication    | \*           | :heavy_check_mark: |
| Division          | /            | :heavy_check_mark: |
| Concatenation     | &#124;&#124; | :heavy_check_mark: |
| Modulo            | mod          |        :x:         |
| Rounding          | round        |        :x:         |
| Truncation        | trunc        |        :x:         |
| Ceiling           | ceil         |        :x:         |
| Floor             | floor        |        :x:         |
| Absolute value    | abs          |        :x:         |
| Exponential       | exp          |        :x:         |
| Natural logarithm | ln           |        :x:         |
| Power             | power        |        :x:         |
| Logarithm         | log          |        :x:         |
| Square root       | sqrt         |        :x:         |

#### VTL-ML - Comparison operators

| Name                  | Symbol           |     Supported      |
| --------------------- | :--------------- | :----------------: |
| Equal to              | =                | :heavy_check_mark: |
| Not equal to          | <>               | :heavy_check_mark: |
| Greater than          | >                | :heavy_check_mark: |
| Less than             | <                | :heavy_check_mark: |
| Greater or equal than | >=               | :heavy_check_mark: |
| Less or equal than    | <=               | :heavy_check_mark: |
| Between               | between          | :heavy_check_mark: |
| Element of            | in / not_in      | :heavy_check_mark: |
| Match characters      | match_characters | :heavy_check_mark: |
| Is null               | isnull           | :heavy_check_mark: |
| Exists in             | exists_in        |        :x:         |

#### VTL-ML - Boolean operators

| Name                  | Symbol |     Supported      |
| --------------------- | :----- | :----------------: |
| Logical conjunction   | and    | :heavy_check_mark: |
| Logical disjunction   | or     | :heavy_check_mark: |
| Exclusive disjunction | xor    | :heavy_check_mark: |
| Logical negation      | not    | :heavy_check_mark: |

#### VTL-ML - Time operators

| Name             | Symbol           | Supported |
| ---------------- | :--------------- | :-------: |
| Period indicator | period_indicator |    :x:    |
| Fill time series | fill_time_series |    :x:    |
| Flow to stock    | flow_to_stock    |    :x:    |
| Stock to flow    | stock_to_flow    |    :x:    |
| Time shift       | timeshift        |    :x:    |
| Time aggregation | time_agg         |    :x:    |
| Actual time      | current_date     |    :x:    |

#### VTL-ML - Set operators

| Name                 | Symbol    | Supported |
| -------------------- | :-------- | :-------: |
| Union                | union     |    :x:    |
| Intersection         | intersect |    :x:    |
| Set difference       | setdiff   |    :x:    |
| Symmetric difference | symdiff   |    :x:    |

#### VTL-ML - Hierarchical aggregation

| Name                 | Symbol    | Supported |
| -------------------- | :-------- | :-------: |
| Hierarchical roll-up | hierarchy |    :x:    |

#### VTL-ML - Aggregate and Analytic operators

| Name                               | Symbol          | Supported |
| ---------------------------------- | :-------------- | :-------: |
| Aggregate invocation               |                 |    :x:    |
| Analytic invocation                |                 |    :x:    |
| Counting the number of data points | count           |    :x:    |
| Minimum value                      | min             |    :x:    |
| Maximum value                      | max             |    :x:    |
| Median value                       | median          |    :x:    |
| Sum                                | sum             |    :x:    |
| Average value                      | avg             |    :x:    |
| Population standard deviation      | stddev_pop      |    :x:    |
| Sample standard deviation          | stddev_samp     |    :x:    |
| Population variance                | var_pop         |    :x:    |
| Sample variance                    | var_samp        |    :x:    |
| First value                        | first_value     |    :x:    |
| Last value                         | last_value      |    :x:    |
| Lag                                | lag             |    :x:    |
| lead                               | lead            |    :x:    |
| Rank                               | rank            |    :x:    |
| Ratio to report                    | ratio_to_report |    :x:    |

#### VTL-ML - Data validation operators

| Name            | Symbol          | Supported |
| --------------- | :-------------- | :-------: |
| Check datapoint | check_datapoint |    :x:    |
| Check hierarchy | check_hierarchy |    :x:    |
| Check           | check           |    :x:    |

#### VTL-ML - Conditional operators

| Name         | Symbol       |     Supported      |
| ------------ | :----------- | :----------------: |
| If Then Else | if-then-else | :heavy_check_mark: |
| Nvl          | nvl          |        :x:         |

#### VTL-ML - Clause operators

| Name                       | Symbol  |     Supported      |
| -------------------------- | :------ | :----------------: |
| Filtering Data Points      | filter  | :heavy_check_mark: |
| Calculation of a Component | calc    | :heavy_check_mark: |
| Aggregation                | aggr    |        :x:         |
| Maintaining Components     | keep    | :heavy_check_mark: |
| Removal of Components      | drop    | :heavy_check_mark: |
| Change of Component name   | rename  | :heavy_check_mark: |
| Pivoting                   | pivot   |        :x:         |
| Unpivoting                 | unpivot |        :x:         |
| Subspace                   | sub     |        :x:         |