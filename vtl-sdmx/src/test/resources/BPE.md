# Permanent Database of Facilities

The permanent database of facilities ([BPE](https://www.insee.fr/en/metadonnees/source/serie/s1161)) is a statistical
database. It lists a wide range of equipments and services,
merchant or not, accessible to the public throughout France on January, 1st each year.

## Prerequisites

In order to simplify the first developments of the use case, we assume that:

- the received DSD (`mes:Structure`) contains all the needed information (code lists, concept schemes, are "
  dereferenced", thanks to the SDMX
  API for instance)
- the corresponding datasets are `sdmx-ml` compatible (identifier tuple are unique)
- refine SDMX / VTL types: only use VTL basic scalar types for now

## Business use case

### Sources

The input source is the finest available database, where a line describes a specific facility.

- metadata: [`DSD_BPE_DETAIL.xml`](./DSD_BPE_DETAIL.xml)
- data: [`BPE_DETAIL.csv`](./BPE_DETAIL_SAMPLE.csv)

`BPE_DETAIL`:

|     Name      | Description         |     SDMX Type      | VTL Type |    Role    |
|:-------------:|---------------------|:------------------:|:--------:|:----------:|
| ID_EQUIPEMENT | Facility identifier |       STRING       |  STRING  | IDENTIFIER |
|    TYPEQU     | Type of facility    | FR1:CL_TYPEQU(1.0) |  STRING  | ATTRIBUTE  |
|    DEPCOM     | Municipality code   | FR1:CL_DEPCOM(1.0) |  STRING  | ATTRIBUTE  |
|   REF_YEAR    | Year                |       STRING       |  STRING  | ATTRIBUTE  |
|   LAMBERT_X   | Facility longitude  |       STRING       |  STRING  |  MEASURE   |
|   LAMBERT_Y   | Facility latitude   |       STRING       |  STRING  |  MEASURE   |

### Step 1: validation of municipality code in input file

VTL script:

```vtl
define datapoint ruleset UNIQUE_MUNICIPALITY (variable DEPCOM) is
    MUNICIPALITY_FORMAT_RULE : match_characters(DEPCOM, "[0-9]{5}|2[A-B][0-9]{3}") errorcode "Municipality code is not in the correct format"
end datapoint ruleset;

CHECK_MUNICIPALITY := check_datapoint(BPE_DETAIL, UNIQUE_MUNICIPALITY invalid);
```

**Check that resulting `CHECK_MUNICIPALITY` contains no observations.**

### Step 2: clean BPE input database

VTL script:

```vtl
BPE_DETAIL_CLEAN := BPE_DETAIL  [drop LAMBERT_X, LAMBERT_Y]
                                [rename ID_EQUIPEMENT to id, TYPEQU to facility_type, DEPCOM to municipality, REF_YEAR to year];
```

`BPE_DETAIL_CLEAN` (temporary dataset):

|     Name      | Description         |  Type  |    Role    |
|:-------------:|---------------------|:------:|:----------:|
|      id       | Facility identifier | STRING | IDENTIFIER |
| facility_type | Type of facility    | STRING | ATTRIBUTE  |
| municipality  | Municipality code   | STRING | ATTRIBUTE  |
|     year      | Year                | STRING | ATTRIBUTE  |

### Step 3: BPE aggregation by municipality, type and year

VTL script:

```vtl
BPE_MUNICIPALITY <- BPE_DETAIL_CLEAN [aggr nb := count(id) group by municipality, year, facility_type];
```

`BPE_MUNICIPALITY` (persistent dataset):

|     Name      | Description          | VTL Type |    Role    |
|:-------------:|----------------------|:--------:|:----------:|
| municipality  | Municipality code    |  STRING  | IDENTIFIER |
| facility_type | Type of facility     |  STRING  | IDENTIFIER |
|     year      | Year                 |  STRING  | IDENTIFIER |
|      nb       | Number of facilities | INTEGER  |  MEASURE   |

**Compare handwritten DSD ([DSD_BPE_TOWN](./DSD_BPE_TOWN.xml)) to that produced by Trevas.**

### Step 4: BPE aggregation by NUTS 3, type and year

VTL script:

```vtl
BPE_NUTS3 <- BPE_MUNICIPALITY   [calc nuts3 := if substr(municipality,1,2) = "97" then substr(municipality,1,3) else substr(municipality,1,2)]    
                                [aggr nb := count(nb) group by year, nuts3, facility_type];
```

`BPE_NUTS3` (persistent dataset):

|     Name      | Description          |  Type   |    Role    |
|:-------------:|----------------------|:-------:|:----------:|
|     nuts3     | NUTS 3 code          | STRING  | IDENTIFIER |
| facility_type | Type of facility     | STRING  | IDENTIFIER |
|     year      | Year                 | STRING  | IDENTIFIER |
|      nb       | Number of facilities | INTEGER |  MEASURE   |

**Compare handwritten DSD ([DSD_BPE_NUTS3](./DSD_BPE_TOWN.xml)) to that produced by Trevas.**

### Step 5: BPE validation of facility types by NUTS 3

For example, check NUTS 3 with less than 10 bowling alleys (facility_type code `F102`).

VTL script:

```vtl
define datapoint ruleset NUTS3_TYPES (variable facility_type, nb) is
    BOWLING_ALLEY_RULE : when facility_type = "F102" then nb > 10 errorcode "Not enough bowling alleys"
end datapoint ruleset;

CHECK_NUTS3_TYPES := check_datapoint(BPE_NUTS3, NUTS3_TYPES invalid);
```

**Check that `CHECK_NUTS3_TYPES` contains 2 observations (nuts3 `2B` & `976`).**

### Step 6: prepare 2021 census dataset by NUTS 3

VTL script:

```vtl
CENSUS_NUTS3_2021 := CENSUS_NUTS3_2021   
                            [rename REF_AREA to nuts3, TIME_PERIOD to year, POP_TOT to pop]
                            [filter year = "2021"]
                            [calc pop := cast(pop, integer)]
                            [drop year, NB_COM, POP_MUNI];
```

`CENSUS_NUTS3_2021` (temporary dataset):

| Name  | Description      |  Type   |    Role    |
|:-----:|------------------|:-------:|:----------:|
| nuts3 | NUTS 3 code      | STRING  | IDENTIFIER |
|  pop  | Legal population | INTEGER |  MEASURE   |

### Step 7: extract dataset on general practitioners from BPE by NUTS 3 in 2021

VTL script:

```vtl
GENERAL_PRACT_NUTS3_2021 := BPE_NUTS3  [filter facility_type = "D201" and year = "2021"]
                                       [drop facility_type, year];
```

`GENERAL_PRACT_NUTS3_2021` (temporary dataset):

| Name  | Description             |  Type   |    Role    |
|:-----:|-------------------------|:-------:|:----------:|
| nuts3 | NUTS 3 code             | STRING  | IDENTIFIER |
|  nb   | Number of practitioners | INTEGER |  MEASURE   |

### Step 8: merge practitioners & legal population datasets by NUTS 3 in 2021 and compute an indicator

VTL script:

```vtl
BPE_CENSUS_NUTS3_2021 <- inner_join(GENERAL_PRACT_NUTS3_2021, CENSUS_NUTS3_2021)
                                [calc pract_per_10000_inhabitants := nb / pop * 10000]
                                [drop nb, pop];
```

`BPE_CENSUS_NUTS3_2021` (persistent dataset):

|            Name             | Description                                |  Type  |    Role    |
|:---------------------------:|--------------------------------------------|:------:|:----------:|
|            nuts3            | NUTS 3 code                                | STRING | IDENTIFIER |
| pract_per_10000_inhabitants | Nb of practitioners per 10 000 inhabitants | NUMBER |  MEASURE   |

**Compare handwritten DSD ([BPE_CENSUS_NUTS3_2021](./DSD_BPE_TOWN.xml)) to that produced by Trevas.**

### Bonus

Instead of using local DSD & data files, consume
from [Insee SDMX indicators service](https://www.insee.fr/en/information/2868055) (Warning: SDMX version)