from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, avg, sum

conf = SparkConf().set("spark.driver.memory", "10G")\
                    .set("spark.executor.memory", "140G")\
                    .set("spark.executor.cores", 39)

sc = SparkContext(master = "spark://c220g2-010609.wisc.cloudlab.us:7077", appName="correlations", conf=conf)
sql_c = SQLContext(sc)

imd_df = sql_c.read.format("csv").option("header","true").option("inferSchema", "true").load("hdfs://master:9000/data/IMD_outer_code.csv")

imd_avg = imd_df.groupBy("category", "outer_code").agg(avg(col('value')).alias('avg_value')).orderBy(col('outer_code'))


pdf = sql_c.read.format("csv").option("header","true").option("inferSchema", "true").load("hdfs://master:9000/data/prescribe.csv")

mor_df = sql_c.read.format("csv").option("header","true").option("inferSchema", "true").load("hdfs://master:9000/data/mortality_by_outer_code.csv")

pop_df = sql_c.read.format("csv").option("header","true").option("inferSchema", "true").load("hdfs://master:9000/data/population_by_outer_code.csv")


pop_avg_heart= pdf.filter(col('bnf_sec') == 2)\
                    .join(pop_df, ["outer_code", "year"])\
                    .groupBy("bnf_sub_sec", "year", "outer_code")\
                    .agg((sum(col("items")) / sum(col("total"))).alias('items'),
                            (sum(col("quantity")) / sum(col("total"))).alias('quantity'),
                            (sum(col("act_cost")) / sum(col("total"))).alias('act_cost'),
                            (sum(col("net_ingredient_cost")) / sum(col("total"))).alias('net_ingredient_cost')
                        ).orderBy(col('outer_code'))

pop_avg_heart.persist()

# locals
PROC = "hdfs://master:9000/data/processed"

categories = imd_df.select("category").distinct().rdd.map(lambda x : x[0]).collect()

prescribe_cols = [ 
    'items',
    "quantity",
    "act_cost",
    "net_ingredient_cost"
]

bnf_sub_secs = {
    1: "Positive Inotropic Drugs ",
    2: "Diuretics",
    3: "Anti-Arrhythmic Drugs",
    4: "Beta-Adrenoceptor Blocking Drugs",
    5: "Hypertension and Heart Failure",
    6: "Nit,Calc Block & Other Antianginal Drugs",
    7: "Sympathomimetics",
    8: "Anticoagulants And Protamine",
    9: "Antiplatelet Drugs",
    10: "Stable Angina, Acute/Crnry Synd&Fibrin",
    11: "Antifibrinolytic Drugs & Haemostatics",
    12: "Lipid-Regulating Drugs",
    13: "Local Sclerosants"
}


res = []

for sub_sec, name in bnf_sub_secs.items():
    df =  pop_avg_heart.filter(col("bnf_sub_sec") == sub_sec).join(imd_avg, ["outer_code"])
    df.persist()
    for cat in categories:
        for column in prescribe_cols:
            corr = df.filter(col('category') == cat).stat.corr('avg_value', column)
            res.append('2,%s,%s,%s,%r' % (name, cat, column, corr))

    df.unpersist()


with open('res.txt', 'w') as ofs:
    ofs.write('bnf_sec,bnf_sub_sec,imd_cat,prescribe_col,corr\n')
    ofs.write('\n'.join(res))





