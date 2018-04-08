import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
jobdetailFrame = glueContext.create_dynamic_frame.from_catalog(database = "ch4qtoncom", table_name = "jobstatus", transformation_ctx = "jobdetailFrame")
jobdetalIncFrame = glueContext.create_dynamic_frame.from_catalog(database = "ch4qtoncom", table_name = "jobdata_c4656cdc3f2cebfcfa104a2a5444fbc8", transformation_ctx = "jobdetalIncFrame")
jobdetailDF = jobdetailFrame.toDF()
jobdetailIncDF = jobdetalIncFrame.toDF()
///918356
jobdetailIncDF_U = jobdetailIncDF.filter(jobdetailIncDF.incr_type=='U').dropDuplicates()
jobdetailIncDF_I = jobdetailIncDF.filter(jobdetailIncDF.incr_type=='I')
jobdetailIncDF_D = jobdetailIncDF.filter(jobdetailIncDF.incr_type=='D')
jobdetailIncDF_U.createOrReplaceTempView("test_updates")
updates_df_nonull = spark.sql("select * from test_updates where where lat!='null' and lng!='null'")
df_update_nonull.createOrReplaceTempView("nonullupdate")
jobdetailIncDF_DI = jobdetailIncDF_D.union(jobdetailIncDF_I)
win = Window().partitionBy("id").orderBy(col("updated_on").asc())
jobdetailIncDF_U_ranked = jobdetailIncDF_U.withColumn("order_inc_rank",rank().over(win))
rows_update_df=jobdetailIncDF_U_ranked.filter(jobdetailIncDF_U_ranked.order_inc_rank == 1)
rows_update_df.createOrReplaceTempView("temprank")
spark.sql("select id,order_inc_rank,count(id) as cnt from temprank where incr_type == 'U' group by id,order_inc_rank having cnt>1 order by cnt desc").show()
win_apt_date= Window().partitionBy("id").orderBy(col("appointmentdate").asc())
jobdetailIncDF_U_ranked_apt = rows_update_df.withColumn("order_inc_rank_apt",rank().over(win_apt_date))
jobdetailIncDF_U_ranked_apt.createOrReplaceTempView("temprank_apt")
spark.sql("select id,order_inc_rank_apt,count(id) as cnt from temprank_apt where incr_type == 'U' group by id,order_inc_rank_apt having cnt>1 order by cnt desc").show()
--------------------------
rows_update_df=jobdetailIncDF_U_ranked.filter(jobdetailIncDF_U_ranked.order_inc_rank == 1).drop('order_inc_rank')
rows_update_df=jobdetailIncDF_U_ranked.filter(jobdetailIncDF_U_ranked.order_inc_rank == 1)
rows_update_df.createOrReplaceTempView("temprank")
spark.sql("select id,updated_on,order_inc_rank from temprank where id = 918356").show()
spark.sql("select id,order_inc_rank,count(id) as cnt from temprank where incr_type == 'U' group by id,order_inc_rank having cnt>1 order by cnt desc").show()
jobdetailIncDF_U_ranked.createOrReplaceTempView("temprank1")
jobdetailIncDF.dropDuplicates()
jobdetailIncDF.createOrReplaceTempView("temprank2")
spark.sql("select count(*) from temprank").show()
spark.sql("select id,count(id) as cnt,order_inc_rank from temprank where incr_type == 'U' group by id,order_inc_rank order by cnt desc").show()
jobdetailIncDF_DI_U_ranked_union = jobdetailIncDF_DI.union(rows_update_df)
spark.sql("select id,count(id) as cnt from temp group by id order by cnt desc").show()
# total = 2217
# unique updates = 1573
#creating temp tables(Optional Can use Dataframe joins aswell)
jobdetailDF.createOrReplaceTempView("jobdetail_fullload")
jobdetailIncDF_DI_U_ranked_union.createOrReplaceTempView("uniontbl")
df_left_join = spark.sql("select * from jobdetail_fullload left join uniontbl on jobdetail_fullload.id=uniontbl.id")
jobdetailDF.createOrReplaceTempView("jobdetail_fullload")
jobdetailIncDF.createOrReplaceTempView("jobdetail_Inc")
##spark.sql("select count(*) from jobdetail_Inc where incr_type = 'I'").show()    
spark.sql("select count(distinct(id)) from jobdetail_Inc where incr_type = 'U'").show()
##Handling updates
df_left_join = spark.sql("select * from jobdetail_fullload left join jobdetail_Inc on jobdetail_fullload.id=jobdetail_Inc.id")
after_del_df = df_left_join.filter(df_left_join.incr_type !='D')
'''Inc_updates_temp = after_del_df.select("jobdetail_Inc.incr_type","jobdetail_Inc.id","jobdetail_fullload.id","jobdetail_fullload.jobid","jobdetail_Inc.jobid","jobdetail_Inc.created_on","jobdetail_Inc.updated_on","jobdetail_fullload.created_on","jobdetail_fullload.updated_on")
df_left_join.createOrReplaceTempView("updates_temp")
spark.sql("select count(*) from updates_temp where incr_type = 'U'").show()'''
##Isolate updates
Inc_updates = after_del_df.select("jobdetail_Inc.incr_type","jobdetail_Inc.id","jobdetail_Inc.created_on","jobdetail_Inc.updated_on","jobdetail_Inc.lock_version","jobdetail_Inc.jobid","jobdetail_Inc.appointmentdate","jobdetail_Inc.appointmenttime","jobdetail_Inc.contactname","jobdetail_Inc.contactphone","jobdetail_Inc.contactemail","jobdetail_Inc.address","jobdetail_Inc.town","jobdetail_Inc.postcode","jobdetail_Inc.accessinfo","jobdetail_Inc.lng","jobdetail_Inc.lat","jobdetail_Inc.earlieststarttime","jobdetail_Inc.lateststarttime","jobdetail_Inc.externaltimeslotid","jobdetail_Inc.externaltimeslotname")
Inc_updates.createOrReplaceTempView("updates")
spark.sql("select count(*) from updates").show()
win = Window().partitionBy("id").orderBy(col("updated_on").asc())
Inc_rank_df = Inc_updates.withColumn("order_inc_rank",rank().over(win))
Inc_rank_df.printSchema()
rows_update_df = Inc_rank_df.filter(Inc_rank_df.order_inc_rank==1).drop('order_inc_rank')
df_src_data = df_left_join.filter(df_left_join.incr_type.isNull())
inc_data_insert_df = jobdetailIncDF.filter(jobdetailIncDF.incr_type=='I').drop('incr_type')
df_src_data_selected = df_src_data.select("jobdetail_fullload.id","jobdetail_fullload.created_on","jobdetail_fullload.updated_on","jobdetail_fullload.lock_version","jobdetail_fullload.jobid","jobdetail_fullload.appointmentdate","jobdetail_fullload.appointmenttime","jobdetail_fullload.contactname","jobdetail_fullload.contactphone","jobdetail_fullload.contactemail","jobdetail_fullload.address","jobdetail_fullload.town","jobdetail_fullload.postcode","jobdetail_fullload.accessinfo","jobdetail_fullload.lng","jobdetail_fullload.lat","jobdetail_fullload.earlieststarttime","jobdetail_fullload.lateststarttime","jobdetail_fullload.externaltimeslotid","jobdetail_fullload.externaltimeslotname")
src_ins_df=df_src_data_selected.union(inc_data_insert_df)
final_df=src_ins_df.union(inc_data_insert_df)
final_df.csv.write("'s3://sms-reporting-rawbucket/taffazzel/JOBDETAILMerged")
