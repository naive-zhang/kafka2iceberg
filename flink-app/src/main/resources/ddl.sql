create table flink_tasks.t_task
(
    task_id     int                                not null comment 'task_id',
    param_key   varchar(512)                       not null comment 'key of a parameter',
    param_value varchar(512)                       not null comment 'value of a parameter',
    create_time datetime default CURRENT_TIMESTAMP null comment 'create time',
    update_time datetime default CURRENT_TIMESTAMP null on update CURRENT_TIMESTAMP comment 'update_time',
    primary key (task_id, param_key)
)
    comment 'flink task' collate = utf8mb4_bin;

create table flink_tasks.t_task_group
(
    task_group_id int auto_increment,
    task_id       int                                not null comment 'task_id',
    task_name     varchar(256)                       not null comment 'name of a task',
    create_time   datetime default CURRENT_TIMESTAMP null comment 'create time',
    update_time   datetime default CURRENT_TIMESTAMP null on update CURRENT_TIMESTAMP comment 'update_time',
    primary key (task_group_id, task_id)
)
    comment 'task group' collate = utf8mb4_bin;

create table flink_tasks.t_task_group_params
(
    task_group_id int                                not null comment 'task_group_id',
    param_key     varchar(512)                       not null comment 'key of a parameter',
    param_value   varchar(512)                       not null comment 'value of a parameter',
    create_time   datetime default CURRENT_TIMESTAMP null comment 'create time',
    update_time   datetime default CURRENT_TIMESTAMP null on update CURRENT_TIMESTAMP comment 'update_time',
    primary key (task_group_id, param_key)
)
    comment 'flink task' collate = utf8mb4_bin;

