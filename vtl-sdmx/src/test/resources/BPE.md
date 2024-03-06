# Permanent Base of Facilities

The permanent database of facilities (BPE) is a statistical database. It lists a wide range of equipment and services,
merchant or not, accessible to the public throughout France on January, 1st each year.

## Technical assumptions

- received `mes:Structure` contains all the needed information (code lists, concepts, are "dereferenced", thanks to SDMX
  API for instance)
- received datasets are `sdmx-ml` compatible (identifier tuple are unique)

## Business use case

### Sources

The input source is the finest available database, where a line describes a piece of equipment.

- metadata: [`DSD_BPE_DETAIL.xml`](./DSD_BPE_DETAIL.xml)
- data: [`BPE_DETAIL.csv`](./BPE_DETAIL_SAMPLE.csv)

`BPE_DETAIL`:

|     Name      | Description         |  Type  |    Role    |
|:-------------:|---------------------|:------:|:----------:|
| ID_EQUIPEMENT | Facility identifier | STRING | IDENTIFIER |
|    TYPEQU     | Type of equipment   | STRING | ATTRIBUTE  |
|    DEPCOM     | Town code           | STRING | ATTRIBUTE  |
|      AN       | Year                | STRING | ATTRIBUTE  |
|   LAMBERT_X   | X geo coordinate    | STRING |  MEASURE   |
|   LAMBERT_Y   | X geo coordinate    | STRING |  MEASURE   |

### Step 1: input town code validation

```vtl
define datapoint ruleset ONLY_TOWN (variable DEPCOM) is
    TOWN_FORMAT_RULE : match_characters(DEPCOM, "[0-9]{5}|2[A-B][0-9]{3}") errorcode "Town code is not in the correct format"
end datapoint ruleset;

CHECK_TOWN := check_datapoint(BPE_DETAIL, ONLY_TOWN invalid);
```

**Check that `CHECK_TOWN` contains no observations.**

### Step 2: clean input database

```vtl
BPE_DETAIL_CLEAN := BPE_DETAIL  [drop LAMBERT_X, LAMBERT_Y]
                                [calc id := ID_EQUIPEMENT, typequ := typequ, town := DEPCOM, year := AN];
```

`BPE_DETAIL_CLEAN` (temporary dataset):

|  Name   | Description         |  Type  |    Role    |
|:-------:|---------------------|:------:|:----------:|
|   id    | Facility identifier | STRING | IDENTIFIER |
| type_eq | Type of equipment   | STRING | ATTRIBUTE  |
|  town   | Town code           | STRING | ATTRIBUTE  |
|  year   | Year                | STRING | ATTRIBUTE  |

### Step 3: communal aggregation by type and year

```vtl
BPE_TOWN <- BPE_DETAIL_CLEAN    [aggr nb := count(id) group by year, town, type_eq];
```

`BPE_TOWN` (persistent dataset):

|  Name   | Description         |  Type   |    Role    |
|:-------:|---------------------|:-------:|:----------:|
|  town   | Town code           | STRING  | IDENTIFIER |
| type_eq | Type of equipment   | STRING  | IDENTIFIER |
|  year   | Year                | STRING  | IDENTIFIER |
|   nb    | Number of equipment | INTEGER |  MEASURE   |

**Compare handwritten DSD ([DSD_BPE_TOWN](./DSD_BPE_TOWN.xml)) to that produced by Trevas.**

### Step 4: nuts3 aggregation by type and year

```vtl
BPE_NUTS3 <- BPE_TOWN   [calc nuts3 := if substr(town,1,2) = "97" then substr(town,1,3) else substr(town,1,2)]    
                        [aggr nb := count(nb) group by year, nuts3, type_eq];
```

`BPE_NUTS3` (persistent dataset):

|  Name   | Description         |  Type   |    Role    |
|:-------:|---------------------|:-------:|:----------:|
|  nuts3  | Nuts 3 code         | STRING  | IDENTIFIER |
| type_eq | Type of equipment   | STRING  | IDENTIFIER |
|  year   | Year                | STRING  | IDENTIFIER |
|   nb    | Number of equipment | INTEGER |  MEASURE   |

**Compare handwritten DSD ([DSD_BPE_NUTS3](./DSD_BPE_TOWN.xml)) to that produced by Trevas.**

### Step 5: nuts3 facility types validation

Check nuts3 with less than 10 bowling alley (type_eq code `F102`).

```vtl
define datapoint ruleset NUTS3_TYPES (variable type_eq, nb) is
    BOWLING_ALLEY_RULE : when type_eq = "F102" then nb > 10 errorcode "Not many bowling alleys"
end datapoint ruleset;

CHECK_NUTS3_TYPES := check_datapoint(BPE_NUTS3, NUTS3_TYPES invalid);
```

**Check that `CHECK_NUTS3_TYPES` contains 2 observations (nuts3 `2B` & `976`).**

### Step 6: prepare legal population by nuts3 in 2021 dataset

```vtl
LEGAL_POP_NUTS3_2021 := LEGAL_POP_NUTS3   
                            [rename REF_AREA to nuts3, TIME_PERIOD to year, POP_TOT to pop]
                            [filter year = "2021"]
                            [drop year, NB_COM, POP_MUNI];
```

`LEGAL_POP_NUTS3_2021` (temporary dataset):

| Name  | Description      |  Type   |    Role    |
|:-----:|------------------|:-------:|:----------:|
| nuts3 | Nuts 3 code      | STRING  | IDENTIFIER |
|  pop  | Total population | INTEGER |  MEASURE   |

### Step 7: prepare doctor dataset from BPE by nuts3 in 2021

```vtl
GENERAL_DOCTOR_NUTS3_2021 := BPE_NUTS3  [filter type_eq = "D201" and year = "2021"]
                                        [drop type_eq, year];
```

`GENERAL_DOCTOR_NUTS3_2021` (temporary dataset):

| Name  | Description         |  Type   |    Role    |
|:-----:|---------------------|:-------:|:----------:|
| nuts3 | Nuts 3 code         | STRING  | IDENTIFIER |
|  nb   | Number of equipment | INTEGER |  MEASURE   |

### Step 8: merge doctor & legal population datasets by nuts3 in 2021 and calculate an indicator

```vtl
BPE_NUTS3_LEGAL_POP_2021 <- inner_join(GENERAL_DOCTOR_NUTS3_2021, LEGAL_POP_NUTS3_2021)
                                [calc doctor_per_10000_inhabitants := nb / pop * 10000]
                                [drop nb, pop];
```

`BPE_NUTS3_LEGAL_POP_2021` (persistent dataset):

|             Name             | Description                         |  Type   |    Role    |
|:----------------------------:|-------------------------------------|:-------:|:----------:|
|            nuts3             | Nuts 3 code                         | STRING  | IDENTIFIER |
| doctor_per_10000_inhabitants | Nb of doctor per 10 000 inhabitants | INTEGER |  MEASURE   |

**Compare handwritten DSD ([DSD_BPE_NUTS3_LEGAL_POP_2021](./DSD_BPE_TOWN.xml)) to that produced by Trevas.**

### Bonus

Instead of using local meta & data files, consume those available on the Insee repository (Warning: SDMX version)