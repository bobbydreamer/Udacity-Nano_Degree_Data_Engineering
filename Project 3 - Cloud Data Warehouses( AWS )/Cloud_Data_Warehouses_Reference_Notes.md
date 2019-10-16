# Cloud Data Warehouses( AWS )

## Warehousing Concepts & Terminologies

### Dimensional Modeling Goals
1. Easy to understand
2. Fast analytical query performance

**Star Schema** : Joins with dimensions only. Good for OLAP not for OLTP   
**3NF** : Lots of expensive joins. Hard to explain business users. 

#### Fact Tables
Tables records business events and columns have data in quantifiable METRICS like a order(quantity), phone call(duration), book review(rating)
Fact columns are usually Quantifiable/Numeric & Additive
Eg:- Total amoount of an invoice could be added to compute total sales(Good fact). Invoice number is numeric makes no sense when you add it(Not a good fact)

#### Dimension Tables
Record the context of the business events(who, what, where, why)
Dimension table columns contain ATTRIBUTES like the store at which an item is purchased, or the customer who made the call, date&time, Physical Locations, Human Roles, Goods Sold. 

#### ETL
1. Extract from 3NF database(Join Tables to get data, discard old data)
2. Transform by cleaning(inconsistencies, duplication, missing values), Tidiness(changing types, adding new columns)
3. Structuring & Load data into facts & dimension tables

#### Kimball architecture
According to Kimball's Bus Architecture, data is kept in a common dimension data model shared across different departments. It does not allow for individual department specific data modeling requirements.
All business process use the same given dimension

**Datamart** : It also contains facts & dimension data model but much smaller & separate than dimensional data model and it is focussed only on one department or business. 
Disadvantage : Inconsistent views and structure across department, due to which its discouraged. 

#### Inmon's Corporate Information Factor(CIF)
Data Acquisition -> Enterprise Data Warehouse -> Data Delivery -> Data Marts -> AT Application  

*CIF Contains*  
* 2 ETL Process
    1. Source Systems -> 3NF DB
    2. 3NF DB -> Departmental Data Marts  
* Enterprise Wide data store(single source of truths for data marts)
* Datamarts are dimensionally modeled

#### OLAP Cube
OLAP Cube is an aggregation of a fact metric on a number of dimensions.   

**OLAP Operations** : 
**Rollup** : Sum up the sales of each city by Country: eg. US, France(less columns in branch dimension)  

**Drill-down** : Decompose the sales of each city into smaller districts(more columns in branch dimension)

> Need for atomic data

**Slice** : Reducing N dimension cube to N-1 dimensions by restricting one dimension to a single value. Eg: month='MAR', looking for data in a specific partition.  

**Dice** : Same dimensions but computing a sub-cube by restricting, some of the values of the dimensions. Eg: month in ['Feb', 'Mar'] and movie in ['Avatar', 'Batman'] and branch='NY', some columns in few partitions.

### Which is better in terms of On premises Vs. Cloud  
* Scalability - Cloud
* Operational Cost - On Premise
* Up front cost - Cloud 
* Elasticity - Cloud 