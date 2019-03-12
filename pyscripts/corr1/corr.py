from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, avg, sum

conf = SparkConf().set("spark.driver.memory", "10G")\
                    .set("spark.executor.memory", "140G")\
                    .set("spark.executor.cores", 39)

sc = SparkContext(master = "spark://c220g2-010609.wisc.cloudlab.us:7077", appName="correlations", conf=conf)
sql_c = SQLContext(sc)

imd_df = sql_c.read.format("csv").option("header","true").option("inferSchema", "true").load("hdfs://master:9000/data/IMD_outer_code.csv")

imd_avg = imd_df.groupBy("category", "outer_code").agg(avg(col('value')).alias('avg_value'))


pdf = sql_c.read.format("csv").option("header","true").option("inferSchema", "true").load("hdfs://master:9000/data/prescribe.csv")

mor_df = sql_c.read.format("csv").option("header","true").option("inferSchema", "true").load("hdfs://master:9000/data/mortality_by_outer_code.csv")

pop_df = sql_c.read.format("csv").option("header","true").option("inferSchema", "true").load("hdfs://master:9000/data/population_by_outer_code.csv")


pop_avg_bnf_sec = pdf.join(pop_df, ["outer_code", "year"])\
                        .groupBy("bnf_sec", "year", "outer_code")\
                        .agg((sum(col("items")) / sum(col("total"))).alias('items'),
                                (sum(col("quantity")) / sum(col("total"))).alias('quantity'),
                                (sum(col("act_cost")) / sum(col("total"))).alias('act_cost'),
                                (sum(col("net_ingredient_cost")) / sum(col("total"))).alias('net_ingredient_cost')
                            )

pop_avg_bnf_sec.show(10)
# locals
PROC = "hdfs://master:9000/data/processed"

categories = imd_df.select("category").distinct().rdd.map(lambda x : x[0]).collect()
prescribe_cols = [ 
    'items',
    "quantity",
    "act_cost",
    "net_ingredient_cost"
]



bnf_secs = {
    1: 'Gastro-Intestinal System',
    2: 'Cardiovascular System ',
    3: 'Respiratory System',
    4: 'Central Nervous System ',
    5: 'Infections',
    6: 'Endocrine System'
}


res = []

for sec, name in bnf_secs.items():
    df =  pop_avg_bnf_sec.filter(col("bnf_sec") == sec).join(imd_avg, ["outer_code"])
    for cat in categories:
        for column in prescribe_cols:
            corr = df.filter(col('category') == cat).stat.corr('avg_value', column)
            res.append('bnf_sec = %s, imd_cat = %s, column = %s, correlation = %r' % (name, cat, column, corr))




with open('res.txt', 'w') as ofs:
    ofs.write('\n'.join(res))





