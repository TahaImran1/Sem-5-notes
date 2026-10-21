**National University of Computer and Emerging Sciences**

		            **Laboratory Manual \# 05**  
			  **Data analysis and visualization**

| Course Instructor | Muhammad Amir Iqbal  |
| :---- | :---- |
| Lab Instructor | Muhammad Hashir Mohsineen |
| Date | 28-Sep-26 |
| Semester | Fall- 26 |

**Objectives:**

* Advance pandas   
* Multi indexing

Data Set: UCI retail  
Helping functions: [https\://docs.google.com/document/d/1KtpM7IoQIbldcVtxchNPWsQ-EO49tPVCJ1llytnoo-0/edit?usp=sharing](https://docs.google.com/document/d/1KtpM7IoQIbldcVtxchNPWsQ-EO49tPVCJ1llytnoo-0/edit?usp=sharing)

1. **Task 0: Setup & Data Cleaning**   
* Load the CSV. Parse InvoiceDate into a real datetime column. Create two derived columns: Date (just the calendar day) and Month (string like "2010-12").

* Identify cancelled orders: InvoiceNo starting with "C". Create df\_clean that excludes them (cancellations have negative Quantity and represent returns, not sales, keep them in a separate df\_cancelled for later, don't just discard).

* Create a Revenue column \= Quantity \* UnitPrice on df\_clean.


Note: df\_clean should have no negative quantities and a working Revenue column.

2. **Task A: MultiIndex Construction**   
* A1: Use set\_index() to make (Country, StockCode, InvoiceDate) the index of a copy of df\_clean. Inspect .index.nlevels, .index.names, and .index.get\_level\_values(0).

* A2: Build a monthly summary: df\_clean.groupby(\['Country', 'StockCode', 'Month'\]).agg(TotalQty=('Quantity','sum'), TotalRevenue=('Revenue','sum')). Confirm the result already has a 3-level MultiIndex. Explicitly (re)name the levels using .index.set\_names(...) even though groupby names them automatically, practice doing it by hand.

* A3: Not every (Country, StockCode, Month) combination in reality had a sale, some products simply didn't sell in some countries in some months, so groupby silently skips those combinations. Use pd.MultiIndex.from\_product(\[countries, stockcodes, months\], names=\[...\]) to build the complete grid, then .reindex() your Task A2 summary against it. Fill missing TotalQty/TotalRevenue with 0\. This is the real analytical value of from\_product: exposing "no sale happened here" as an explicit zero.

* A4: Use pd.MultiIndex.from\_tuples() to build a small custom index of 5 specific (Country, StockCode) pairs (pairs of your choice). Select just those rows out of your Task A2 result using .loc\[\].

3. **Task B: Selecting MultiIndex Data** 

Use the Task A2 (or A3) summary for this part.

* B1: Partial indexing: get all rows for 'Germany' using .loc\['Germany'\]. What happened to the Country level in the result?  
* B2: Full tuple indexing: get all rows for ('France', '85123A'):  every month, one country, one product.  
* B3: pd.IndexSlice: get every country's data for StockCodes '21730' through '22752' (lexicographic slice) across all months, in one .loc\[\] call using pd.IndexSlice.  
* B4: .xs():  
  * Extract all countries' and products' data for Month \== '2010-12', without dropping the other levels, and again with them dropped.   
  * Extract all data for StockCode \== '22633' across every country and month using .xs(level='StockCode') do this without resetting the index first.

4. **Task C: Sorting & Index Management**   
* C1: Take your Task A2 summary (built from groupby, so it should already be sorted) and deliberately shuffle it with .sample(frac=1). Try slicing it with pd.IndexSlice the way you did in Task 3\. What error do you get, and why do pandas need a sorted (lexsorted) index for this kind of slice?  
    
* C2: Fix it with .sort\_index(). Confirm with .index.is\_monotonic\_increasing.  
    
* C3: reset\_index() your sorted summary into a flat DataFrame. This is what you'd do before exporting to CSV or merging with a non-indexed lookup table. Build a tiny Country \-\> Region mapping table (e.g., UK/EIRE \-\> "UK & Ireland", France/Germany/Netherlands/Spain \-\> "Continental Europe") as a plain 2-column DataFrame, and merge() it onto your flattened summary.  
    
* C4: Re-set\_index() back to (Country, StockCode, Month) including the new Region, decide whether Region belongs inside the index or as a regular column, and justify your choice in one sentence( add comment).

5. **Task D: Reshaping**   
* D1: Starting from the Task A2 summary, unstack('StockCode') so that products become columns and (Country, Month) remains the row index. You now have a wide table of TotalRevenue per product.  
    
* D2: stack() your D1 result back down. Confirm (with .equals() or a manual check) that you're back to a tidy long format equivalent to what you started with.  
    
* D3: Skip the groupby entirely this time: go straight from df\_clean using pivot\_table(index='Country', columns='Month', values='Revenue', aggfunc='sum', margins=True). What does the margins=True row/column represent?  
    
* D4: Take your D3 pivot table and melt() it back into a tidy long format: one row per (Country, Month, Revenue). Compare this to the output of D2.

6. **Task E: MultiIndex Columns**   
* E1: On df\_clean, run df\_clean.groupby(\['Country', 'Month'\]).agg({'Revenue': \['sum', 'mean', 'count'\], 'Quantity': \['sum', 'max'\]}). Inspect .columns, you now have a MultiIndex on the column axis, not just the rows.  
    
* E2: Access the ('Revenue', 'mean') column two ways: with tuple indexing (df\[('Revenue','mean')\]) and with .xs('mean', axis=1, level=1).  
    
* E3: Flatten the hierarchical columns into single strings like 'Revenue\_sum', 'Quantity\_max' using a list comprehension over df.columns. This is what you'd do before handing the table to a non-pandas consumer (e.g., exporting to CSV, feeding a plotting library that expects flat columns).  
    
* E4: Take your E1 result and unstack('Country'). You now have both a MultiIndex on the columns from the .agg() and the unstacked Country level added on top: a 3-level column hierarchy. Pull out just the sum values for 'Germany'.

7. **Task F: Level-wise Analysis**   
* F1: groupby(level='Country').sum() on your Task A2 summary: total revenue and quantity per country, collapsing product and month.  
    
* F2: groupby(level=\['Country','Month'\]).sum(),  same, but keep month.  
    
* F3: (the payoff task) For each (Country, StockCode, Month) row, compute what percentage of that country's total revenue in that month came from that specific product. This requires aligning a group-level total back onto every row that belongs to the group:  do it with groupby(level=\['Country','Month'\])\['TotalRevenue'\].transform('sum'), then divide.  
    
* F4: For each country, find the single product with the highest revenue share in December 2010\. (filter to Month \== '2010-12', then group and use idxmax() on the share column from F3.)

8. **Task G:**

Include the following:

* A tidy long-format table: (Country, Month, StockCode, TotalRevenue, RevenueShare) reuse Part F.   
* A wide summary table with MultiIndex columns: Country as the top level, ('Dec\_2010', 'Jan\_2011') revenue as the second level, for the top-5 products overall (by total revenue across both months).  
* One pd.IndexSlice query that pulls, in a single call, the December and January rows for only the two countries with the highest overall revenue.

