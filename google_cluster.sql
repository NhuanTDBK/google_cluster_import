create table task_events(
	time_id bigint,
	missing_info integer,
	job_id bigint,
	task_index bigint,
	machine_id bigint,
	event_type integer,
	user varchar(255),
	scheduling_class integer,
	priority integer,
	cpu_request float,
	memory_request float,
	disk_space_request float,
	different_machine tinyint
);
create table machine_events(
	time_id bigint,
	machine_id bigint,
	event_type integer,
	platform_id varchar(255),
	cpus float,
	memory float
);
create table job_events(
	time_id bigint,
	missing_info integer,
	job_id bigint,
	event_type varchar(255),
	user varchar(255),
	scheduling_class integer,
	job_name varchar(255),
	logical_job_name varchar(255)
);
create table machine_attributes(
	time_id bigint,
	machine_id bigint,
	attribute_name text,
	attribute_value text,
	attribute_deleted tinyint
);
create table task_constraint(
	time_id bigint,
	job_id bigint,
	task_index bigint,
	comparison_operator integer,
	attribute_name varchar(255),
	attribute_value varchar(255)
);
CREATE TABLE `task_usage` (
  `start_time` bigint(20) DEFAULT NULL,
  `end_time` bigint(20) DEFAULT NULL,
  `job_id` bigint(20) DEFAULT NULL,
  `task_index` bigint(20) DEFAULT NULL,
  `machine_id` bigint(20) DEFAULT NULL,
  `cpu_rate` float DEFAULT NULL,
  `canonical_memory` float DEFAULT NULL,
  `assigned_memory` float DEFAULT NULL,
  `ummapped_page_cache` float DEFAULT NULL,
  `total_page_cache` float DEFAULT NULL,
  `maximum_memory_usage` float DEFAULT NULL,
  `disk_io_time` float DEFAULT NULL,
  `local_disk_space_usage` float DEFAULT NULL,
  `maximum_cpu_rate` float DEFAULT NULL,
  `maximum_diskio_time` float DEFAULT NULL,
  `cycles_per_instructor` float DEFAULT NULL,
  `memory_access` float DEFAULT NULL,
  `sample_portion` float DEFAULT NULL,
  `aggregation_type` tinyint(4) DEFAULT NULL,
  `sampled_cpu_usage` float DEFAULT NULL
);

