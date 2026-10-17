**National University of Computer and Emerging Sciences**

		            **Laboratory Manual \# 03**  
			  **Data analysis and visualization**

| Course Instructor | Muhammad Amir Iqbal  |
| :---- | :---- |
| Lab Instructor | Muhammad Hashir Mohsineen |
| Date | 07-Sep-26 |
| Semester | Fall- 26 |

**Title: Building Your First Medallion Pipeline in Databricks**   
**Objectives:**

* Navigate the Databricks Free Edition workspace and manage notebooks.  
* Ingest raw JSON logs into a Bronze Delta table.  
* Clean and transform data into a structured Silver Delta table.  
* Execute a Gold business aggregation table.  
* Observe the difference between Full Overwrite and Incremental Append loads.

1. **Task 1: Create a Databricks Notebook**   
1. Log in to your Databricks Free Edition workspace.  
2. In the left sidebar, click Workspace \-\> Home.  
3. Click Create \-\> Notebook (create button will be on the right side of the window).  
4. Name it **medallion\_pipeline\_lab**, set the default language to Python

	Test the spark environment:  
	

| print("Spark Version:", spark.version)display(spark.range(10)) |
| :---- |

**![][image1]**

2. **Task 2: Import Products JSON into Databricks**   
   1. Select Catalog \-\> workspace \-\> default  
   2. Then click on create and select volume  
   3.  Create a new volume named as products  
   4. Then select the newly created products volume and click on upload to this volume (top right corner)  
   5. ![][image2]  
   6. Copy path of the uploaded json data file  
   7. Run the following command to check if your data is uploaded and working correctly  
      products\_file \= "Copied data path"  
      display(spark.read.option("multiLine", "true").json(products\_file))  
        
3. **Task 3: extract the products array**  
   import json  
     
   with open("data path") as f:  
       data \= json.load(f)  
     
   products \= data\["products"\]  
     
   print("Total products:", len(products))  
   **Define explicit schema to avoid mixed data type issue:**   
   from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, ArrayType, MapType  
     
   \# Define schema to handle mixed int/float types  
   schema \= StructType(\[  
       StructField("id", IntegerType(), True),  
       StructField("title", StringType(), True),  
       StructField("description", StringType(), True),  
       StructField("category", StringType(), True),  
       StructField("price", DoubleType(), True),  
       StructField("discountPercentage", DoubleType(), True),  \# Mixed int/float in source  
       StructField("rating", DoubleType(), True),  
       StructField("stock", IntegerType(), True),  
       StructField("tags", ArrayType(StringType()), True),  
       StructField("brand", StringType(), True),  
       StructField("sku", StringType(), True),  
       StructField("weight", IntegerType(), True),  
       StructField("dimensions", MapType(StringType(), DoubleType()), True),  
       StructField("warrantyInformation", StringType(), True),  
       StructField("shippingInformation", StringType(), True),  
       StructField("availabilityStatus", StringType(), True),  
       StructField("reviews", ArrayType(MapType(StringType(), StringType())), True),  
       StructField("returnPolicy", StringType(), True),  
       StructField("minimumOrderQuantity", IntegerType(), True),  
       StructField("meta", MapType(StringType(), StringType()), True),  
       StructField("images", ArrayType(StringType()), True),  
       StructField("thumbnail", StringType(), True)  
   \])  
   **create a Spark DataFrame**  
   bronze\_raw\_df \= spark.createDataFrame(products, schema\=schema)  
     
   display(bronze\_raw\_df)  
   **View schema:** bronze\_raw\_df.printSchema()  
   **View dataframe:** display(bronze\_raw\_df.limit(10))  
     
4. **Task 4:  Write the Bronze Delta Table**  

	A useful Bronze table should record when the data was ingested.   
	from pyspark.sql.functions import current\_timestamp, lit

bronze\_df \= (  
    bronze\_raw\_df  
    .withColumn("ingestion\_time", current\_timestamp())  
    .withColumn("source", lit("DummyJSON Products API"))  
)  
Inspect: display(bronze\_df)

Bronze delta table  
bronze\_df.write \\  
    .format("delta") \\  
    .mode("overwrite") \\  
    .saveAsTable("bronze\_products")  
Verify:

| display(spark.sql("""    SELECT \*    FROM bronze\_products    LIMIT 10""")) |
| :---- |

1. spark.sql(""" **write query to print number of records** """).show() 

5. **Task 5: Silver Layer \- Data Cleansing & Schema Enforcement**  
   The Bronze layer is not necessarily convenient for analytics.  
   We now need to:  
1. Select relevant columns.  
2. Rename columns where necessary.  
3. Enforce data types.  
4. Calculate additional fields.  
5. Handle nested data.  
6. Filter invalid records.

**Create a Silver DataFrame**  
	from pyspark.sql.functions import col

silver\_df \= (  
    spark.table("bronze\_products")  
    .select(  
        col("id").cast("integer").alias("product\_id"),  
        col("title"),  
        col("category"),  
        col("brand"),  
        col("price").cast("double"),  
        col("discountPercentage").cast("double").alias("discount\_percentage"),  
        col("rating").cast("double"),  
        col("stock").cast("integer"),  
        col("availabilityStatus").alias("availability\_status"),  
        col("minimumOrderQuantity").cast("integer").alias("minimum\_order\_quantity"),  
        col("ingestion\_time"),  
        col("source")  
    )  
)  
**Data Quality Filtering**   
Suppose our business rules are:  
price \> 0  
rating between 0 and 5  
stock \>= 0

**Apply the rules:**   
silver\_df \= (  
    silver\_df  
    .filter(col("price") \> 0)  
    .filter((col("rating") \>= 0) & (col("rating") \<= 5))  
    \# apply filter on stock  
)  
**Check result:**

| print("Valid products:", silver\_df.count()) |
| :---- |

**Calculate Discounted Price:**  
We can create a useful business field:    
discounted\_price \= price × (1 \- discount\_percentage / 100\)  
from pyspark.sql.functions import round

silver\_df \= silver\_df.withColumn(  
    "discounted\_price",  
    round(  
        col("price") \* (1 \- col("discount\_percentage") / 100),  
        2  
    )  
)  
Display:  
display(  
    silver\_df.select(  
        "product\_id",  
        "title",  
        "price",  
        "discount\_percentage",  
        "discounted\_price"  
    )  
)  
Create a Stock Status:  
from pyspark.sql.functions import when

silver\_df \= silver\_df.withColumn(  
    "stock\_status",  
    when(col("stock") \== 0, "Out of Stock")  
    \#apply condition for low stock, "Low Stock")  
    .otherwise("In Stock")  
)

1. Display: product id, title, stock, stock status

**Save the Silver Table**  

1. Write the cleaned dataset (like you did in task 4 silver\_df.write )  
   2. Verify the dataset and print schema

6. **Task 6: Gold Layer** 

The Gold layer answers specific business questions.  
Instead of storing individual products, we will create summaries.

**Product Summary by Category**  
How many products exist in each category?    
from pyspark.sql.functions import count

gold\_category\_df \= (  
    spark.table("silver\_products")  
    .groupBy("category")  
    .agg(  
        count("product\_id").alias("total\_products")  
    )  
    .orderBy("total\_products", ascending\=False)  
)

1. Display gold\_category\_df  
2. \# Overwrite Gold report table save as gold\_category\_summary

**Brand performance:**  
from pyspark.sql.functions import (  
    count,  
    avg,  
    sum as \_sum,  
    max as \_max,  
    min as \_min  
)  
gold\_brand\_df \= (  
    spark.table("silver\_products")  
    .groupBy("brand")  
    .agg(  
        count("product\_id").alias("total\_products"),  
        round(avg("rating"), 2).alias("average\_rating"),  
        round(avg("price"), 2).alias("average\_price"),  
        \_sum("stock").alias("total\_stock")  
    )  
    .orderBy("total\_products", ascending\=False)  
)

gold\_brand\_df.write \\  
    .format("delta") \\  
    .mode("overwrite") \\  
    .saveAsTable("gold\_brand\_summary")

1. Display table

7. **Task 7: Incremental Ingestion with Record Update**

**Generate a Simulated Day 2 Batch:**  
from pyspark.sql.functions import lit, current\_timestamp

day2\_data \= \[  
    \# New Product 195  
    {  
        "id": 195,  
        "title": "Smart Fitness Watch",  
        "category": "mobile-accessories",  
        "brand": "TechFit",  
        "price": 79.99,  
        "discountPercentage": 10.0,  
        "rating": 4.3,  
        "stock": 120  
    },

    \# New Product 196  
    {  
        "id": 196,  
        "title": "Wireless Gaming Mouse",  
        "category": "laptops",  
        "brand": "GamePro",  
        "price": 49.99,  
        "discountPercentage": 15.0,  
        "rating": 4.5,  
        "stock": 85  
    },

    \# New Product 197  
    {  
        "id": 197,  
        "title": "Portable Bluetooth Speaker",  
        "category": "mobile-accessories",  
        "brand": "SoundMax",  
        "price": 59.99,  
        "discountPercentage": 12.0,  
        "rating": 4.1,  
        "stock": 60  
    },

    \# New Product 198  
    {  
        "id": 198,  
        "title": "USB-C Fast Charger",  
        "category": "mobile-accessories",  
        "brand": "PowerTech",  
        "price": 29.99,  
        "discountPercentage": 5.0,  
        "rating": 4.6,  
        "stock": 200  
    },

    \# Update to existing Product 1  
    {  
        "id": 1,  
        "title": "Essence Mascara Lash Princess \- Updated",  
        "category": "beauty",  
        "brand": "Essence",  
        "price": 12.99,  
        "discountPercentage": 15.0,  
        "rating": 4.2,  
        "stock": 75  
    },

    \# Update to existing Product 2  
    {  
        "id": 2,  
        "title": "Eyeshadow Palette with Mirror \- Updated",  
        "category": "beauty",  
        "brand": "Glamour Beauty",  
        "price": 24.99,  
        "discountPercentage": 20.0,  
        "rating": 4.4,  
        "stock": 50  
    }  
\]

**Convert the Incoming Batch to a DataFrame**   
day2\_df \= spark.createDataFrame(day2\_data)

day2\_df \= (  
    day2\_df  
    .withColumn("ingestion\_batch", lit("day2"))  
    .withColumn("ingestion\_time", current\_timestamp())  
)

display(day2\_df)

**Append the Incoming Batch to Bronze**   
The Bronze layer is designed to preserve incoming/raw data.  
Therefore, we can append the new batch to Bronze.

1. First, create a separate incremental Bronze table from the original Bronze data   
   Expected 194 products  
   bronze\_df \= spark.table("bronze\_products")  
     
   bronze\_df \= (  
       bronze\_df  
       .withColumn("ingestion\_batch", lit("day1"))  
   )  
     
   bronze\_df.write \\  
       .format("delta") \\  
       .mode("overwrite") \\  
       .saveAsTable("bronze\_incremental\_products")  
     
   spark.sql("""  
   SELECT COUNT(\*) AS total\_records  
   FROM bronze\_incremental\_products  
   """).show()  
     
2. append the six incoming records:   
   day2\_df.write \\  
       .format("delta") \\  
       .mode("append") \\  
       .saveAsTable("bronze\_incremental\_products")  
     
   spark.sql("""  
   SELECT COUNT(\*) AS total\_records  
   FROM bronze\_incremental\_products  
   """).show()

**Examine the Bronze History**   
display(  
    spark.sql("""  
        SELECT  
            id,  
            title,  
            price,  
            stock,  
            ingestion\_batch  
        FROM bronze\_incremental\_products  
        WHERE id IN (1, 2)  
        ORDER BY id, ingestion\_batch  
    """)  
)  
**![][image3]**

 

8. **Task 8: Silver Layer Upsert (`MERGE INTO`)**

Create the cleaned Day 2 DataFrame:   
from pyspark.sql.functions import col, round

day2\_cleaned\_df \= (  
    day2\_df  
    .filter(col("price") \> 0)  
    .filter((col("rating") \>= 0) & (col("rating") \<= 5))  
    .filter(col("stock") \>= 0)  
    .withColumn(  
        "product\_id",  
        col("id").cast("integer")  
    )  
    .withColumn(  
        "price",  
        col("price").cast("double")  
    )  
    .withColumn(  
        "discount\_percentage",  
        col("discountPercentage").cast("double")  
    )  
    .withColumn(  
        "rating",  
        col("rating").cast("double")  
    )  
    .withColumn(  
        "stock",  
        col("stock").cast("integer")  
    )  
    .withColumn(  
        "discounted\_price",  
        round(  
            col("price") \*  
            (1 \- col("discountPercentage") / 100),  
            2  
        )  
    )  
    .select(  
        "product\_id",  
        "title",  
        "category",  
        "brand",  
        "price",  
        "discount\_percentage",  
        "discounted\_price",  
        "rating",  
        "stock",  
        "ingestion\_batch",  
        "ingestion\_time"  
    )  
)  
**Perform the MERGE** 

1. You have to create silver\_incremental\_products 

from delta.tables import DeltaTable  
silver\_table \= DeltaTable.forName(  
    spark,  
    "silver\_incremental\_products"  
)  
(  
    silver\_table.alias("target")  
    .merge(  
        day2\_cleaned\_df.alias("source"),  
        "target.product\_id \= source.product\_id"  
    )  
    .whenMatchedUpdateAll()  
    .whenNotMatchedInsertAll()  
    .execute()  
)

**Verify the upsert:**  
display(  
    spark.sql("""  
        SELECT  
            product\_id,  
            title,  
            price,  
            discount\_percentage,  
            discounted\_price,  
            rating,  
            stock  
        FROM silver\_incremental\_products  
        WHERE product\_id IN (1, 2, 195, 196, 197, 198)  
        ORDER BY product\_id  
    """)  
)  
**Verify There Are No Duplicate Product IDs**   
%sql

SELECT  
    product\_id,  
    COUNT(\*) AS occurrences  
FROM silver\_incremental\_products  
GROUP BY product\_id  
HAVING COUNT(\*) \> 1;

9. **Task 9: Refresh the Gold Layer** 

Now that Silver contains the latest product information, we need to refresh our Gold business metrics.

Create the Gold aggregation:

from pyspark.sql.functions import (

    count,

    avg,

    sum as \_sum

)

gold\_updated\_df \= (

    spark.table("silver\_incremental\_products")

    .groupBy("category")

    .agg(

        count("product\_id").alias("total\_products"),

        round(avg("price"), 2).alias("average\_price"),

        round(avg("rating"), 2).alias("average\_rating"),

        \_sum("stock").alias("total\_stock")

    )

    .orderBy("total\_products", ascending\=False)

)

1. Display the gold\_update\_df and save as gold\_incremental\_category\_summary: expected 24 rows

![][image4]

10. **Task 10: Verification**

Write a query to count the records of bronze\_incremental\_products. Expected 200  
Write a query to to count the records of silver\_incremental\_products : expected 198

---

[image1]: <data:image/png;base64,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>

[image2]: <data:image/png;base64,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>

[image3]: <data:image/png;base64,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>

[image4]: <data:image/png;base64,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>