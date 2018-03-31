from pyspark.sql.types import *

task_events = StructType([
	StructField("time_id",LongType,nullable=False),
	StructField("missing_info",IntegerType,nullable=False),
	StructField("job_id",LongType,nullable=False),
	StructField("task_index",LongType,nullable=True),
	StructField("machine_id",LongType,nullable=True),
	StructField("event_type",IntegerType,nullable=False),
	StructField("user",StringType,nullable=False),
	StructField("scheduling_class",IntegerType,nullable=False),
	StructField("priority",IntegerType,nullable=False),
	StructField("cpu_request",FloatType,nullable=False),
	StructField("memory_request",FloatType,nullable=False),
	StructField("disk_space_request",IntegerType,nullable=False)
])
