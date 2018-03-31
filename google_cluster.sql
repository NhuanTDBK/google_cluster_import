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

