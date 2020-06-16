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

| Name                       | Symbol             | Supported |
| -------------------------- | :----------------- | :-------: |
| String concatenation       | &#124;&#124;       |    :x:    |
| Whitespace removal         | trim, rtrim, ltrim |    :x:    |
| Character case conversion  | upper/lower        |    :x:    |
| Sub-string extraction      | substr             |    :x:    |
| String pattern replacement | replace            |    :x:    |
| String pattern location    | instr              |    :x:    |
| String length              | length             |    :x:    |

#### VTL-ML - Numeric operators

| Name              | Symbol |     Supported      |
| ----------------- | :----- | :----------------: |
| Unary plus        | +      |        :x:         |
| Unary minus       | -      |        :x:         |
| Addition          | +      | :heavy_check_mark: |
| Subtraction       | -      | :heavy_check_mark: |
| Multiplication    | \*     | :heavy_check_mark: |
| Division          | /      | :heavy_check_mark: |
| Concatenation     | \|\|   | :heavy_check_mark: |
| Modulo            | mod    |        :x:         |
| Rounding          | round  |        :x:         |
| Truncation        | trunc  |        :x:         |
| Ceiling           | ceil   |        :x:         |
| Floor             | floor  |        :x:         |
| Absolute value    | abs    |        :x:         |
| Exponential       | exp    |        :x:         |
| Natural logarithm | ln     |        :x:         |
| Power             | power  |        :x:         |
| Logarithm         | log    |        :x:         |
| Square root       | sqrt   |        :x:         |

#### VTL-ML - Comparison operators

| Name             | Symbol           | Supported |
| ---------------- | :--------------- | :-------: |
| Equal to         | =                |    :x:    |
| Not equal to     | <>               |    :x:    |
| Greater than     | > >=             |    :x:    |
| Less than        | < <=             |    :x:    |
| Between          | between          |    :x:    |
| Element of       | in / not_in      |    :x:    |
| Match characters | match_characters |    :x:    |
| Is null          | isnull           |    :x:    |
| Exists in        | exists_in        |    :x:    |

#### VTL-ML - Boolean operators

| Name                  | Symbol |     Supported      |
| --------------------- | :----- | :----------------: |
| Logical conjunction   | and    | :heavy_check_mark: |
| Logical disjunction   | or     | :heavy_check_mark: |
| Exclusive disjunction | xor    | :heavy_check_mark: |
| Logical negation      | not    |        :x:         |

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

| Name         | Symbol       | Supported |
| ------------ | :----------- | :-------: |
| If Then Else | if-then-else |    :x:    |
| Nvl          | nvl          |    :x:    |

#### VTL-ML - Clause operators

| Name                       | Symbol  | Supported |
| -------------------------- | :------ | :-------: |
| Filtering Data Points      | filter  |    :x:    |
| Calculation of a Component | calc    |    :x:    |
| Aggregation                | aggr    |    :x:    |
| Maintaining Components     | keep    |    :x:    |
| Removal of Components      | drop    |    :x:    |
| Change of Component name   | rename  |    :x:    |
| Pivoting                   | pivot   |    :x:    |
| Unpivoting                 | unpivot |    :x:    |
| Subspace                   | sub     |    :x:    |
