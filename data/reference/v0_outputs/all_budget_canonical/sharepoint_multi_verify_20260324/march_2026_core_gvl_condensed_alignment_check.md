# March 2026 Core GVL Condensed Alignment Check

Only rows where the existing/new split is meaningful are shown here.

- `new_correct_existing_wrong`: the 2026 new-door logic is right, but the existing side is still off.
- `both_existing_and_new_wrong`: both sides are still off against the GVL reference.
- `fully_aligned`: existing and new both line up, allowing only small rounding noise under 0.5 k.

| Category | Plain English | Region | Sub Region | Salesperson | Workbook Existing k | GVL Existing k | Delta Existing k | Workbook New k | GVL New k | Delta New k | Workbook Total k | GVL Total k | Delta Total k |
|:--|:--|:--|:--|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| new_correct_existing_wrong | New is correct, but existing is still off. | Benelux | NL Central | Marjelein | 93.02 | 98.032 | -5.012 | 4 | 4 | 0 | 97.02 | 102 | -4.98 |
| new_correct_existing_wrong | New is correct, but existing is still off. | Benelux | NL Other | -Kein Vertriebsmitarbeiter- | 1.1 | 2.7 | -1.6 | 0 | 0 | 0 | 1.1 | 3 | -1.9 |
| new_correct_existing_wrong | New is correct, but existing is still off. | France | France South | Yannick | 5 | 4 | 1 | 4 | 4 | 0 | 9 | 8 | 1 |
| new_correct_existing_wrong | New is correct, but existing is still off. | Germany | Bayern | I. Papoulias | 61.494 | 60.932 | 0.562 | 4 | 4 | 0 | 65.494 | 65 | 0.494 |
| new_correct_existing_wrong | New is correct, but existing is still off. | Germany | DE Other | -Kein Vertriebsmitarbeiter- | 11.66 | 23.32 | -11.66 | 0 | 0 | 0 | 11.66 | 24 | -12.34 |
| new_correct_existing_wrong | New is correct, but existing is still off. | Switzerland | German Switzerland | Christiane | 67.086 | 70.094 | -3.008 | 4 | 4.2 | -0.2 | 71.086 | 74 | -2.914 |
| both_existing_and_new_wrong | Both existing and new are still off. | France | France North | Elena | 2.5 | 5 | -2.5 | 5 | 10 | -5 | 7.5 | 16 | -8.5 |
| fully_aligned | Existing and new both line up. | Benelux | NL Other + BL | Gabrielle | 102.472 | 102.462 | 0.01 | 4 | 4 | 0 | 106.472 | 106 | 0.472 |
| fully_aligned | Existing and new both line up. | Germany | NRW - Marina | Marina | 59.414 | 59.414 | -0 | 4 | 4 | 0 | 63.414 | 63 | 0.414 |
| fully_aligned | Existing and new both line up. | Germany | NRW - Ulrike | Ulrike | 15.793 | 15.793 | -0 | 3 | 3 | 0 | 18.793 | 19 | -0.207 |
| fully_aligned | Existing and new both line up. | Germany | North | Kerstin | 65.389 | 65.389 | -0 | 5 | 5 | 0 | 70.389 | 70 | 0.389 |
| fully_aligned | Existing and new both line up. | Germany | North East | Aracelli | 40.545 | 40.545 | 0 | 4 | 4 | 0 | 44.545 | 45 | -0.455 |
| fully_aligned | Existing and new both line up. | Germany | Retail | Aracelli | 20 | 20 | 0 | 0 | 0 | 0 | 20 | 20 | 0 |
| fully_aligned | Existing and new both line up. | Germany | South West | Sibylle | 185.382 | 185.382 | -0 | 4 | 4 | 0 | 189.382 | 189 | 0.382 |
| fully_aligned | Existing and new both line up. | Italy | Italy | Elena | 1.5 | 1.5 | 0 | 4 | 4 | 0 | 5.5 | 6 | -0.5 |
| fully_aligned | Existing and new both line up. | Spain | Spain | Montse | 17.5 | 17.5 | 0 | 4 | 4 | 0 | 21.5 | 22 | -0.5 |
| fully_aligned | Existing and new both line up. | Switzerland | French Switzerland | Elena | 3.5 | 3.675 | -0.175 | 5 | 5.25 | -0.25 | 8.5 | 9 | -0.5 |