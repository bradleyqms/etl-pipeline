# March 2026 Workbook vs CSV Report Tables

## Processed Management (Report Table Grain)

| Market Group   | Region                     | Workbook View       | Status         |   Workbook k (Mar-2026) |   CSV k (Mar-2026) |   Delta k (Workbook-CSV) |
|:---------------|:---------------------------|:--------------------|:---------------|------------------------:|-------------------:|-------------------------:|
| Core Markets   | Benelux                    | core_markets_budget | matched        |                     197 |                211 |                      -14 |
| Core Markets   | France                     | core_markets_budget | matched        |                       8 |                 14 |                       -6 |
| Core Markets   | Germany                    | core_markets_budget | matched        |                     460 |                470 |                      -10 |
| Core Markets   | Italy                      | core_markets_budget | matched        |                       2 |                  6 |                       -4 |
| Core Markets   | Spain                      | core_markets_budget | matched        |                      18 |                 22 |                       -4 |
| Core Markets   | Switzerland                | core_markets_budget | matched        |                      71 |                 83 |                      -12 |
| Export         | Distributor - APAC         | export_budget       | matched        |                       0 |                  0 |                        0 |
| Export         | Distributor - Austria      | export_budget       | matched        |                      39 |                 39 |                        0 |
| Export         | Distributor - China        | export_budget       | matched        |                      26 |                 26 |                        0 |
| Export         | Distributor - Middle East  | export_budget       | matched        |                      18 |                 18 |                        0 |
| Export         | Distributor - New          | export_budget       | matched        |                       0 |                  0 |                        0 |
| Export         | Distributor - Other EU     | export_budget       | matched        |                      46 |                 46 |                        0 |
| Export         | Distributor - Other ROW    | export_budget       | matched        |                       3 |                  3 |                        0 |
| Export         | Distributor - Russia       | export_budget       | matched        |                       0 |                  0 |                        0 |
| Export         | Distributor - South Africa | export_budget       | matched        |                      75 |                 75 |                        0 |
| Export         | Export - Direct business   | export_budget       | matched        |                       3 |                  3 |                        0 |
| UK             | Global eTailers            | uk_budget           | matched        |                       3 |                  3 |                        0 |
| UK             | Retail                     | uk_budget           | matched        |                      26 |                 26 |                        0 |
| UK             | Spa                        | uk_budget           | matched        |                      16 |                 16 |                        0 |
| USA            | Amazon                     | nan                 | reference_only |                     nan |                  9 |                      nan |
| USA            | Own eCommerce              | nan                 | reference_only |                     nan |                 34 |                      nan |
| USA            | Retail                     | nan                 | reference_only |                     nan |                  2 |                      nan |
| USA            | Spa                        | nan                 | reference_only |                     nan |                 80 |                      nan |
| eCommerce      | eCommerce (excl. USA)      | nan                 | reference_only |                     nan |                 61 |                      nan |

## Core GVL Salesperson (Report Table Grain)

| Region      | Sub Region         | Salesperson                 | Status   |   Workbook Total k |   GVL Total k |   Delta k |   GVL Existing k |   GVL New k |
|:------------|:-------------------|:----------------------------|:---------|-------------------:|--------------:|----------:|-----------------:|------------:|
| Benelux     | NL Central         | Marjelein                   | matched  |            93.0201 |           102 |  -8.97992 |           98.032 |        4    |
| Benelux     | NL Other           | -Kein Vertriebsmitarbeiter- | matched  |             1.1    |             3 |  -1.9     |            2.7   |        0    |
| Benelux     | NL Other + BL      | Gabrielle                   | matched  |           102.472  |           106 |  -3.52762 |          102.462 |        4    |
| France      | France North       | Elena                       | matched  |             2.5    |            16 | -13.5     |            5     |       10    |
| France      | France South       | Yannick                     | matched  |             5      |             8 |  -3       |            4     |        4    |
| Germany     | Bayern             | I. Papoulias                | matched  |            61.4939 |            65 |  -3.50608 |           60.932 |        4    |
| Germany     | DE Other           | -Kein Vertriebsmitarbeiter- | matched  |            11.66   |            24 | -12.34    |           23.32  |        0    |
| Germany     | NRW - Marina       | Marina                      | matched  |            59.4137 |            63 |  -3.58628 |           59.414 |        4    |
| Germany     | NRW - Ulrike       | Ulrike                      | matched  |            15.793  |            19 |  -3.207   |           15.793 |        3    |
| Germany     | North              | Kerstin                     | matched  |            65.389  |            70 |  -4.61104 |           65.389 |        5    |
| Germany     | North East         | Aracelli                    | matched  |            40.5453 |            45 |  -4.45474 |           40.545 |        4    |
| Germany     | Retail             | Aracelli                    | matched  |            20      |            20 |   0       |           20     |        0    |
| Germany     | South West         | Sibylle                     | matched  |           185.382  |           189 |  -3.61818 |          185.382 |        4    |
| Italy       | Italy              | Elena                       | matched  |             1.5    |             6 |  -4.5     |            1.5   |        4    |
| Spain       | Spain              | Montse                      | matched  |            17.5    |            22 |  -4.5     |           17.5   |        4    |
| Switzerland | French Switzerland | Elena                       | matched  |             3.5    |             9 |  -5.5     |            3.675 |        5.25 |
| Switzerland | German Switzerland | Christiane                  | matched  |            67.0861 |            74 |  -6.91392 |           70.094 |        4.2  |

## USA Region (Report Table Grain)

| Region    | Status         |   Workbook k |   USA CSV k |   Delta k | Notes                                                                |
|:----------|:---------------|-------------:|------------:|----------:|:---------------------------------------------------------------------|
| Central   | matched        |        16.74 |       21.74 |     -5    | USA source column is labeled kUSD but behaves like whole USD values. |
| Northeast | matched        |         8.91 |       13.91 |     -5    | USA source column is labeled kUSD but behaves like whole USD values. |
| Other     | canonical_only |         8.1  |      nan    |    nan    | USA source column is labeled kUSD but behaves like whole USD values. |
| Southeast | matched        |        15.48 |       29.93 |    -14.45 | USA source column is labeled kUSD but behaves like whole USD values. |
| West      | matched        |        24.93 |       20.48 |      4.45 | USA source column is labeled kUSD but behaves like whole USD values. |