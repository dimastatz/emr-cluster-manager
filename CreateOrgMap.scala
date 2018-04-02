spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "---")
spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey","---")

val logs = spark.read.parquet("s3a://---@sg-logs.e1.usw1.opendns.com/querylog-parquet/2018/03/27/00/known/*").select("origin_id")

val label = udf(() => "label")
val organization_id = udf( () =>  scala.util.Random.nextInt(5000) + 1 )
val origin_type_id = udf( () => scala.util.Random.alphanumeric.take(1).mkString("") )

val identities = logs
                    .select("origin_id")
                    .distinct()
                    .withColumn("organization_id", organization_id())
                    .withColumn("label", label())
                    .withColumn("origin_type_id", origin_type_id())

identities.cache()

identities
    .coalesce(1)
    .write
    .option("header", "true")
    .option("delimiter","\t")
    .mode("overwrite")
    .csv("s3a://dima-open-dns/out_id")

var deviceCount:Int = 0

val device = udf(() => {
    deviceCount = deviceCount + 1 
    deviceCount
})

val tenanat = udf(() => "t0000000000000000000000000000000")
val fakeOrigin = udf(() => 1)

identities
    .select("organization_id").distinct
    .withColumn("origin_id", fakeOrigin())
    .withColumn("tenant_id", tenanat())
    .withColumn("device_id", device())
    .coalesce(1)
    .write
    .option("header", "true")
    .option("delimiter","\t")
    .mode("overwrite")
    .csv("s3a://dima-open-dns/org_map")

