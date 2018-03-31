from pyspark.sql.types import LongType, IntegerType, FloatType, StringType, StructType, StructField

task_events = StructType([
    StructField("time_id", LongType(), nullable=True),
    StructField("missing_info", IntegerType(), nullable=True),
    StructField("job_id", LongType(), nullable=True),
    StructField("task_index", LongType(), nullable=True),
    StructField("machine_id", LongType(), nullable=True),
    StructField("event_type", IntegerType(), nullable=True),
    StructField("user", StringType(), nullable=True),
    StructField("scheduling_class", IntegerType(), nullable=True),
    StructField("priority", IntegerType(), nullable=True),
    StructField("cpu_request", FloatType(), nullable=True),
    StructField("memory_request", FloatType(), nullable=True),
    StructField("disk_space_request", IntegerType(), nullable=False)
])
machine_events = StructType([
    StructField("time_id", LongType(), nullable=True),
    StructField("machine_id", LongType(), nullable=True),
    StructField("event_type", IntegerType(), nullable=True),
    StructField("platform_id", StringType(), nullable=True),
    StructField("cpus", FloatType(), nullable=True),
    StructField("memory", FloatType(), nullable=True),
])
job_events = StructType([
    StructField("time_id", LongType(), nullable=True),
    StructField("missing_info", IntegerType(), nullable=True),
    StructField("job_id", LongType(), nullable=True),
    StructField("event_type", StringType(), nullable=True),
    StructField("user", StringType(), nullable=True),
    StructField("scheduling_class", IntegerType(), nullable=True),
    StructField("job_name", StringType(), nullable=True),
    StructField("logical_job_name", StringType(), nullable=True),
])
machine_attributes = StructType([
    StructField("time_id", LongType(), nullable=True),
    StructField("machine_id", LongType(), nullable=True),
    StructField("attribute_name", StringType(), nullable=True),
    StructField("attribute_value", StringType(), nullable=True),
    StructField("attribute_deleted", IntegerType(), nullable=True),
])
task_constraints = StructType([
    StructField("time_id", LongType(), nullable=True),
    StructField("job_id", LongType(), nullable=True),
    StructField("task_index", LongType(), nullable=True),
    StructField("comparision_operator", IntegerType(), nullable=True),
    StructField("attribute_name", StringType(), nullable=True),
    StructField("attribute_value", StringType(), nullable=True),
])
task_usage = StructType([
    StructField("start_time", LongType(), nullable=True),
    StructField("end_time", LongType(), nullable=True),
    StructField("job_id", LongType(), nullable=True),
    StructField("task_index", LongType(), nullable=True),
    StructField("machine_id", LongType(), nullable=True),
    StructField("cpu_rate", LongType(), nullable=True),
    StructField("canonical_memory", FloatType(), nullable=True),
    StructField("assigned_memory", FloatType(), nullable=True),
    StructField("ummapped_page_cache", FloatType(), nullable=True),
    StructField("total_page_cache", FloatType(), nullable=True),
    StructField("maximum_memory_usage", FloatType(), nullable=True),
    StructField("disk_io_time", FloatType(), nullable=True),
    StructField("local_disk_space_usage", FloatType(), nullable=True),
    StructField("maximum_cpu_rate", FloatType(), nullable=True),
    StructField("maximum_diskio_time", FloatType(), nullable=True),
    StructField("cycles_per_instructor", FloatType(), nullable=True),
    StructField("memory_access", FloatType(), nullable=True),
    StructField("sample_portio", FloatType(), nullable=True),
    StructField("aggregation_type", FloatType(), nullable=True),
    StructField("sampled_cpu_usage", FloatType(), nullable=True),

])