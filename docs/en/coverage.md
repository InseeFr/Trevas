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

| Name | Symbol                                       | InMemory | Spark |
| ---- | :------------------------------------------- | :------: | :---: |
| Join | inner_join, left_join, full_join, cross_join |   :x:    |  :x:  |

#### VTL-ML - String operators

| Name                       | Symbol             |       String       | Component | Dataset |
| -------------------------- | :----------------- | :----------------: | :-------: | :-----: |
| String concatenation       | &#124;&#124;       | :heavy_check_mark: |    :x:    |   :x:   |
| Whitespace removal         | trim, rtrim, ltrim | :heavy_check_mark: |    :x:    |   :x:   |
| Character case conversion  | upper/lower        | :heavy_check_mark: |    :x:    |   :x:   |
| Sub-string extraction      | substr             | :heavy_check_mark: |    :x:    |   :x:   |
| String pattern replacement | replace            | :heavy_check_mark: |    :x:    |   :x:   |
| String pattern location    | instr              | :heavy_check_mark: |    :x:    |   :x:   |
| String length              | length             | :heavy_check_mark: |    :x:    |   :x:   |

#### VTL-ML - Numeric operators

| Name              | Symbol       |       Number       | Component | Dataset |
| ----------------- | :----------- | :----------------: | :-------: | :-----: |
| Unary plus        | +            | :heavy_check_mark: |    :x:    |   :x:   |
| Unary minus       | -            | :heavy_check_mark: |    :x:    |   :x:   |
| Addition          | +            | :heavy_check_mark: |    :x:    |   :x:   |
| Subtraction       | -            | :heavy_check_mark: |    :x:    |   :x:   |
| Multiplication    | \*           | :heavy_check_mark: |    :x:    |   :x:   |
| Division          | /            | :heavy_check_mark: |    :x:    |   :x:   |
| Concatenation     | &#124;&#124; | :heavy_check_mark: |    :x:    |   :x:   |
| Modulo            | mod          | :heavy_check_mark: |    :x:    |   :x:   |
| Rounding          | round        | :heavy_check_mark: |    :x:    |   :x:   |
| Truncation        | trunc        | :heavy_check_mark: |    :x:    |   :x:   |
| Ceiling           | ceil         | :heavy_check_mark: |    :x:    |   :x:   |
| Floor             | floor        | :heavy_check_mark: |    :x:    |   :x:   |
| Absolute value    | abs          | :heavy_check_mark: |    :x:    |   :x:   |
| Exponential       | exp          | :heavy_check_mark: |    :x:    |   :x:   |
| Natural logarithm | ln           | :heavy_check_mark: |    :x:    |   :x:   |
| Power             | power        | :heavy_check_mark: |    :x:    |   :x:   |
| Logarithm         | log          | :heavy_check_mark: |    :x:    |   :x:   |
| Square root       | sqrt         | :heavy_check_mark: |    :x:    |   :x:   |

#### VTL-ML - Comparison operators

| Name                  | Symbol           |       Scalar       | Component | Dataset |
| --------------------- | :--------------- | :----------------: | :-------: | :-----: |
| Equal to              | =                | :heavy_check_mark: |    :x:    |   :x:   |
| Not equal to          | <>               | :heavy_check_mark: |    :x:    |   :x:   |
| Greater than          | >                | :heavy_check_mark: |    :x:    |   :x:   |
| Less than             | <                | :heavy_check_mark: |    :x:    |   :x:   |
| Greater or equal than | >=               | :heavy_check_mark: |    :x:    |   :x:   |
| Less or equal than    | <=               | :heavy_check_mark: |    :x:    |   :x:   |
| Between               | between          | :heavy_check_mark: |    :x:    |   :x:   |
| Element of            | in / not_in      | :heavy_check_mark: |    :x:    |   :x:   |
| Match characters      | match_characters | :heavy_check_mark: |    :x:    |   :x:   |
| Is null               | isnull           | :heavy_check_mark: |    :x:    |   :x:   |
| Exists in             | exists_in        | :heavy_check_mark: |    :x:    |   :x:   |

#### VTL-ML - Boolean operators

| Name                  | Symbol |      Boolean       | Component | Dataset |
| --------------------- | :----- | :----------------: | :-------: | :-----: |
| Logical conjunction   | and    | :heavy_check_mark: |    :x:    |   :x:   |
| Logical disjunction   | or     | :heavy_check_mark: |    :x:    |   :x:   |
| Exclusive disjunction | xor    | :heavy_check_mark: |    :x:    |   :x:   |
| Logical negation      | not    | :heavy_check_mark: |    :x:    |   :x:   |

#### VTL-ML - Time operators

| Name             | Symbol           | Time_period | Component | Dataset |
| ---------------- | :--------------- | :---------: | :-------: | :-----: |
| Period indicator | period_indicator |     :x:     |    :x:    |   :x:   |
| Fill time series | fill_time_series |     :x:     |    :x:    |   :x:   |
| Flow to stock    | flow_to_stock    |     :x:     |    :x:    |   :x:   |
| Stock to flow    | stock_to_flow    |     :x:     |    :x:    |   :x:   |
| Time shift       | timeshift        |     :x:     |    :x:    |   :x:   |
| Time aggregation | time_agg         |     :x:     |    :x:    |   :x:   |
| Actual time      | current_date     |     :x:     |    :x:    |   :x:   |

#### VTL-ML - Set operators

| Name                 | Symbol    |      InMemory      | Spark |
| -------------------- | :-------- | :----------------: | :---: |
| Union                | union     | :heavy_check_mark: |  :x:  |
| Intersection         | intersect |        :x:         |  :x:  |
| Set difference       | setdiff   |        :x:         |  :x:  |
| Symmetric difference | symdiff   |        :x:         |  :x:  |

#### VTL-ML - Hierarchical aggregation

| Name                 | Symbol    | InMemory | Spark |
| -------------------- | :-------- | :------: | ----- |
| Hierarchical roll-up | hierarchy |   :x:    | :x:   |

#### VTL-ML - Aggregate and Analytic operators

| Name                               | Symbol          |      InMemory      | Spark |
| ---------------------------------- | :-------------- | :----------------: | :---: |
| Aggregate invocation               |                 |        :x:         |  :x:  |
| Analytic invocation                |                 |        :x:         |  :x:  |
| Counting the number of data points | count           | :heavy_check_mark: |  :x:  |
| Minimum value                      | min             |        :x:         |  :x:  |
| Maximum value                      | max             |        :x:         |  :x:  |
| Median value                       | median          |        :x:         |  :x:  |
| Sum                                | sum             | :heavy_check_mark: |  :x:  |
| Average value                      | avg             | :heavy_check_mark: |  :x:  |
| Population standard deviation      | stddev_pop      |        :x:         |  :x:  |
| Sample standard deviation          | stddev_samp     |        :x:         |  :x:  |
| Population variance                | var_pop         |        :x:         |  :x:  |
| Sample variance                    | var_samp        |        :x:         |  :x:  |
| First value                        | first_value     |        :x:         |  :x:  |
| Last value                         | last_value      |        :x:         |  :x:  |
| Lag                                | lag             |        :x:         |  :x:  |
| lead                               | lead            |        :x:         |  :x:  |
| Rank                               | rank            |        :x:         |  :x:  |
| Ratio to report                    | ratio_to_report |        :x:         |  :x:  |

#### VTL-ML - Data validation operators

| Name            | Symbol          | Supported |
| --------------- | :-------------- | :-------: |
| Check datapoint | check_datapoint |    :x:    |
| Check hierarchy | check_hierarchy |    :x:    |
| Check           | check           |    :x:    |

#### VTL-ML - Conditional operators

| Name         | Symbol       |      Boolean       | Component | Dataset |
| ------------ | :----------- | :----------------: | :-------: | :-----: |
| If Then Else | if-then-else | :heavy_check_mark: |    :x:    |   :x:   |
| Nvl          | nvl          |        :x:         |    :x:    |   :x:   |

#### VTL-ML - Clause operators

| Name                       | Symbol  |      InMemory      |       Spark        |
| -------------------------- | :------ | :----------------: | :----------------: |
| Filtering Data Points      | filter  | :heavy_check_mark: | :heavy_check_mark: |
| Calculation of a Component | calc    | :heavy_check_mark: | :heavy_check_mark: |
| Aggregation                | aggr    |        :x:         |        :x:         |
| Maintaining Components     | keep    | :heavy_check_mark: | :heavy_check_mark: |
| Removal of Components      | drop    | :heavy_check_mark: | :heavy_check_mark: |
| Change of Component name   | rename  | :heavy_check_mark: | :heavy_check_mark: |
| Pivoting                   | pivot   |        :x:         |        :x:         |
| Unpivoting                 | unpivot |        :x:         |        :x:         |
| Subspace                   | sub     |        :x:         |        :x:         |
