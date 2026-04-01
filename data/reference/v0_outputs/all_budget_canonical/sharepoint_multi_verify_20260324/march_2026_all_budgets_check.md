# March 2026 All Budgets Check

## Plain English

- Workbook Existing k means the old business plus any 2025 new doors that have rolled into the base.
- Workbook New k means only the 2026 new doors.
- Only Core Markets has a reference file that splits existing and new separately.
- UK, USA, Export, and processed-management checks can only be compared on total value because their reference files do not split existing vs new.
- For UK specifically, the comparable reference check is the EUR summary view, so the split columns are blank there on purpose.
- If Delta New k is zero, the new-budget logic is working for that row.
- If Delta Total k is still off while Delta New k is zero, the remaining difference is coming from the existing side.

## Core Markets Split Check

| Region | Sub Region | Salesperson | Status | Workbook Existing k | GVL Existing k | Delta Existing k | Workbook New k | GVL New k | Delta New k | Workbook Total k | GVL Total k | Delta Total k | Plain English |
|:--|:--|:--|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|:--|
| Benelux | NL Central | Marjelein | matched | 93.02 | 98.032 | -5.012 | 4 | 4 | 0 | 97.02 | 102 | -4.98 | New matches; the remaining gap is on the existing side. |
| Benelux | NL Other | -Kein Vertriebsmitarbeiter- | matched | 1.1 | 2.7 | -1.6 | 0 | 0 | 0 | 1.1 | 3 | -1.9 | New matches; the remaining gap is on the existing side. |
| Benelux | NL Other + BL | Gabrielle | matched | 102.472 | 102.462 | 0.01 | 4 | 4 | 0 | 106.472 | 106 | 0.472 | Existing and new both line up. What remains is just rounding. |
| France | France North | Elena | matched | 2.5 | 5 | -2.5 | 5 | 10 | -5 | 7.5 | 16 | -8.5 | Both the existing side and the new side differ from the reference. |
| France | France South | Yannick | matched | 5 | 4 | 1 | 4 | 4 | 0 | 9 | 8 | 1 | New matches; the remaining gap is on the existing side. |
| Germany | Bayern | I. Papoulias | matched | 61.494 | 60.932 | 0.562 | 4 | 4 | 0 | 65.494 | 65 | 0.494 | New matches; the remaining gap is on the existing side. |
| Germany | DE Other | -Kein Vertriebsmitarbeiter- | matched | 11.66 | 23.32 | -11.66 | 0 | 0 | 0 | 11.66 | 24 | -12.34 | New matches; the remaining gap is on the existing side. |
| Germany | NRW - Marina | Marina | matched | 59.414 | 59.414 | -0 | 4 | 4 | 0 | 63.414 | 63 | 0.414 | Existing and new both line up. What remains is just rounding. |
| Germany | NRW - Ulrike | Ulrike | matched | 15.793 | 15.793 | -0 | 3 | 3 | 0 | 18.793 | 19 | -0.207 | Existing and new both line up. What remains is just rounding. |
| Germany | North | Kerstin | matched | 65.389 | 65.389 | -0 | 5 | 5 | 0 | 70.389 | 70 | 0.389 | Existing and new both line up. What remains is just rounding. |
| Germany | North East | Aracelli | matched | 40.545 | 40.545 | 0 | 4 | 4 | 0 | 44.545 | 45 | -0.455 | Existing and new both line up. What remains is just rounding. |
| Germany | Retail | Aracelli | matched | 20 | 20 | 0 | 0 | 0 | 0 | 20 | 20 | 0 | Existing and new both line up. What remains is just rounding. |
| Germany | South West | Sibylle | matched | 185.382 | 185.382 | -0 | 4 | 4 | 0 | 189.382 | 189 | 0.382 | Existing and new both line up. What remains is just rounding. |
| Italy | Italy | Elena | matched | 1.5 | 1.5 | 0 | 4 | 4 | 0 | 5.5 | 6 | -0.5 | Existing and new both line up. What remains is just rounding. |
| Spain | Spain | Montse | matched | 17.5 | 17.5 | 0 | 4 | 4 | 0 | 21.5 | 22 | -0.5 | Existing and new both line up. What remains is just rounding. |
| Switzerland | French Switzerland | Elena | matched | 3.5 | 3.675 | -0.175 | 5 | 5.25 | -0.25 | 8.5 | 9 | -0.5 | Existing and new both line up. What remains is just rounding. |
| Switzerland | German Switzerland | Christiane | matched | 67.086 | 70.094 | -3.008 | 4 | 4.2 | -0.2 | 71.086 | 74 | -2.914 | Both the existing side and the new side differ from the reference. |

## UK, USA, Export, and Region-Level Totals

| Check Type | Market Group | Region | Status | Workbook Existing k | Workbook New k | Workbook Total k | Reference Total k | Delta Total k | Plain English |
|:--|:--|:--|:--|--:|--:|--:|--:|--:|:--|
| processed_management | Core Markets | Benelux | matched | 197 | 8 | 204.592 | 211 | -6.408 | Reference only gives one total number, so this row checks the total only. |
| processed_management | Core Markets | France | matched | 8 | 9 | 16.5 | 14 | 2.5 | Reference only gives one total number, so this row checks the total only. |
| processed_management | Core Markets | Germany | matched | 460 | 24 | 483.677 | 470 | 13.677 | Reference only gives one total number, so this row checks the total only. |
| processed_management | Core Markets | Italy | matched | 2 | 4 | 5.5 | 6 | -0.5 | Reference only gives one total number, so this row checks the total only. |
| processed_management | Core Markets | Spain | matched | 18 | 4 | 21.5 | 22 | -0.5 | Reference only gives one total number, so this row checks the total only. |
| processed_management | Core Markets | Switzerland | matched | 71 | 9 | 79.586 | 83 | -3.414 | Reference only gives one total number, so this row checks the total only. |
| processed_management | Export | Distributor - APAC | matched |  |  | 0 | 0 | 0 | Reference only gives one total number, so this row checks the total only. |
| processed_management | Export | Distributor - Austria | matched |  |  | 38.7 | 39 | -0.3 | Reference only gives one total number, so this row checks the total only. |
| processed_management | Export | Distributor - China | matched |  |  | 26.4 | 26 | 0.4 | Reference only gives one total number, so this row checks the total only. |
| processed_management | Export | Distributor - Middle East | matched |  |  | 18 | 18 | 0 | Reference only gives one total number, so this row checks the total only. |
| processed_management | Export | Distributor - New | matched |  |  | 0 | 0 | 0 | Reference only gives one total number, so this row checks the total only. |
| processed_management | Export | Distributor - Other EU | matched |  |  | 45.75 | 46 | -0.25 | Reference only gives one total number, so this row checks the total only. |
| processed_management | Export | Distributor - Other ROW | matched |  |  | 3 | 3 | 0 | Reference only gives one total number, so this row checks the total only. |
| processed_management | Export | Distributor - Russia | matched |  |  | 0 | 0 | 0 | Reference only gives one total number, so this row checks the total only. |
| processed_management | Export | Distributor - South Africa | matched |  |  | 75 | 75 | 0 | Reference only gives one total number, so this row checks the total only. |
| processed_management | Export | Export - Direct business | matched |  |  | 2.88 | 3 | -0.12 | Reference only gives one total number, so this row checks the total only. |
| processed_management | UK | Global eTailers | matched |  |  | 2.9 | 3 | -0.1 | Reference only gives one total number, so this row checks the total only. |
| processed_management | UK | Retail | matched |  |  | 25.416 | 26 | -0.584 | Reference only gives one total number, so this row checks the total only. |
| processed_management | UK | Spa | matched |  |  | 16.704 | 16 | 0.704 | Reference only gives one total number, so this row checks the total only. |
| usa_region | USA | Central | matched | 17 | 5 | 21.74 | 21.74 | 0 | Reference only gives one total number, so this row checks the total only. |
| usa_region | USA | Northeast | matched | 9 | 5 | 13.91 | 13.91 | 0 | Reference only gives one total number, so this row checks the total only. |
| usa_region | USA | Other | canonical_only | 8 | 0 | 8.1 |  |  | No matching reference row exists for this workbook row. |
| usa_region | USA | Southeast | matched | 15 | 5 | 20.48 | 29.93 | -9.45 | Reference only gives one total number, so this row checks the total only. |
| usa_region | USA | West | matched | 25 | 5 | 29.93 | 20.48 | 9.45 | Reference only gives one total number, so this row checks the total only. |