# Databricks notebook source
# 1. إعدادات اسم حساب التخزين والمفتاح السري
storage_account_name = "goldmarketdatalake2026"
storage_account_key = "qob+11c3BAVzaqw7DhtlR2rOroIb7TB2QGNut8aNTSIXM8cE8IsLogoYbj2LwszxwmdBy3zA0bep+AStil/9Ow=="

# 2. تفعيل الربط في جلسة الـ Spark
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

print("✅ تم الربط بحساب التخزين بنجاح والمحرك جاهز لقراءة البيانات!")

# COMMAND ----------

# 1. تحديد مسار ملف الـ CSV الخام في كونتينر الـ bronze
bronze_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/Gold_Price_Mubasher_2013_2026.csv"

# 2. قراءة الملف باستخدام PySpark مع التعرف التلقائي على أنواع البيانات (InferSchema)
df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(bronze_path)

# 3. كتابة البيانات الخام كجدول Delta في طبقة الـ Bronze
df_raw.write.format("delta").mode("overwrite").saveAsTable("bronze_gold_prices")

print("✅ تم قراءة الملف الخام وحفظه بنجاح في الـ Bronze Layer كجدول Delta!")
# عرض عينة من البيانات للتأكد
display(df_raw.limit(5))

# COMMAND ----------

# كود سريع لعرض أسماء الأعمدة الفعلية في جدول الـ Bronze
spark.read.table("bronze_gold_prices").printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, round, expr

# 1. قراءة البيانات من طبقة الـ Bronze
df_bronze = spark.read.table("bronze_gold_prices")

# 2. تحويل البيانات باستخدام try_cast عبر دالة expr لتجنب مشاكل الـ Import
df_silver = df_bronze \
    .withColumn("Gold_Close", expr("try_cast(Close as double)")) \
    .withColumn("Gold_High", expr("try_cast(High as double)")) \
    .withColumn("Gold_Low", expr("try_cast(Low as double)")) \
    .withColumn("Gold_Open", expr("try_cast(Open as double)")) \
    .withColumn("Daily_Spread", round(expr("try_cast(High as double)") - expr("try_cast(Low as double)"), 2)) \
    .filter(col("Gold_Close").isNotNull()) \
    .select("Date", "Gold_Open", "Gold_High", "Gold_Low", "Gold_Close", "Daily_Spread")

# 3. حفظ البيانات النظيفة في طبقة الـ Silver كجدول Delta
df_silver.write.format("delta").mode("overwrite").saveAsTable("silver_gold_prices")

print("✅ تم تخطي الأزمة بنجاح وحفظ البيانات النظيفة في الـ Silver Layer!")
# عرض عينة للتأكد
display(df_silver.limit(5))

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofmonth, quarter, date_format, col, avg, when

# 1. قراءة البيانات من طبقة الـ Silver
df_silver_source = spark.read.table("silver_gold_prices")

# -------------------------------------------------------------------------
# 1️⃣ البعد الأول: Dim_Date (جدول أبعاد الوقت بالتفصيل)
# -------------------------------------------------------------------------
df_dim_date = df_silver_source.select("Date").distinct() \
    .withColumn("Year", year(col("Date"))) \
    .withColumn("Month", month(col("Date"))) \
    .withColumn("Day", dayofmonth(col("Date"))) \
    .withColumn("Quarter", quarter(col("Date"))) \
    .withColumn("Day_Name", date_format(col("Date"), "EEEE")) \
    .withColumn("Month_Name", date_format(col("Date"), "MMMM"))

# -------------------------------------------------------------------------
# 2️⃣ البعد الثاني: Dim_Market_Status (تحليل سلوك وحالة السوق اليومية)
# -------------------------------------------------------------------------
# بنحدد حالة السوق بناءً على الـ Daily Spread (الفرق بين أعلى وأقل سعر) والاتجاه
df_dim_market = df_silver_source.select("Date", "Daily_Spread", "Gold_Open", "Gold_Close") \
    .withColumn("Market_Volatility", 
                when(col("Daily_Spread") > 30, "High Volatility")
                .when(col("Daily_Spread") > 10, "Medium Volatility")
                .otherwise("Low/Stable")) \
    .withColumn("Price_Trend", 
                when(col("Gold_Close") > col("Gold_Open"), "Bullish (Up)")
                .when(col("Gold_Close") < col("Gold_Open"), "Bearish (Down)")
                .otherwise("No Change")) \
    .select(col("Date").alias("Market_Status_Key"), "Market_Volatility", "Price_Trend")

# -------------------------------------------------------------------------
# 3️⃣ البعد الثالث: Dim_Inflation_Proxy (مؤشر التضخم السنوي بناءً على أساس السعر)
# -------------------------------------------------------------------------
# بنحسب متوسط السعر لكل سنة عشان نطلع نسبة نمو السعر (اللي بتعكس التضخم وانخفاض قيمة العملة)
yearly_avg = df_silver_source.groupBy(year("Date").alias("Avg_Year")).agg(avg("Gold_Close").alias("Yearly_Avg_Price"))

df_dim_inflation = df_dim_date.join(yearly_avg, df_dim_date.Year == yearly_avg.Avg_Year, "left") \
    .withColumn("Inflation_Risk_Index", 
                when(col("Yearly_Avg_Price") > 1500, "High Inflation Period")
                .when(col("Yearly_Avg_Price") > 1100, "Moderate Inflation Period")
                .otherwise("Stable Economic Period")) \
    .select(col("Date").alias("Inflation_Key"), "Yearly_Avg_Price", "Inflation_Risk_Index")

# -------------------------------------------------------------------------
# 4️⃣ جدول الحقائق المركزي: Fact_Gold_Prices
# -------------------------------------------------------------------------
df_fact_gold = df_silver_source.select(
    col("Date").alias("Date_Key"),           # يربط مع Dim_Date
    col("Date").alias("Market_Status_Key"),   # يربط مع Dim_Market_Status
    col("Date").alias("Inflation_Key"),       # يربط مع Dim_Inflation_Proxy
    "Gold_Open", 
    "Gold_High", 
    "Gold_Low", 
    "Gold_Close", 
    "Daily_Spread"
)

# -------------------------------------------------------------------------
# 💾 حفظ الـ 4 جداول رسميًا بصيغة Delta في الـ Metastore
# -------------------------------------------------------------------------
df_dim_date.write.format("delta").mode("overwrite").saveAsTable("dim_gold_date")
df_dim_market.write.format("delta").mode("overwrite").saveAsTable("dim_gold_market_status")
df_dim_inflation.write.format("delta").mode("overwrite").saveAsTable("dim_gold_inflation")
df_fact_gold.write.format("delta").mode("overwrite").saveAsTable("fact_gold_prices")

print("==========================================================================")
print("👑 الله ينور يا عبد العال! تم بناء Data Warehouse ضخم وStar Schema احترافية بـ 3 أبعاد!")
print("==========================================================================")

# COMMAND ----------

# كود سريع لعرض الجداول الحالية المتسجلة في الداتابريكس
print("✨ الجداول المتوفرة في مستودع البيانات الحالي:")
spark.sql("SHOW TABLES").show()

