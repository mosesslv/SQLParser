-- AI SQLReview SQL记录表
drop table `ai_sr_sql_detail`;
CREATE TABLE `ai_sr_sql_detail` (
  `id`                      int NOT NULL AUTO_INCREMENT,
  `created_by`              varchar(128) NOT NULL,
  `created_at`              datetime NOT NULL,
  `updated_by`              varchar(128) NOT NULL,
  `updated_at`              datetime NOT NULL,
  `sequence`                varchar(128) NOT NULL COMMENT '标识SQL唯一序列',
  `tenant_code`             varchar(128) NOT NULL COMMENT '租户标识',
  `db_type`                 varchar(128) NOT NULL COMMENT 'ORACLE | MYSQL ......',
  `db_conn_url`             varchar(512) NULL COMMENT 'DB 连接串',
  `schema_name`             varchar(128) NOT NULL COMMENT 'SCHEMA名称',
  `sql_text`                text NOT NULL COMMENT 'SQL 文本',
  `statement`               varchar(128) NULL COMMENT 'SELECT | UPDATE | DELETE | INSERT ......',
  `dynamic_mosaicking`      varchar(128) NULL COMMENT '是否使用动态拼接 YES | NO',
  `table_names`             varchar(4096) NULL COMMENT 'SQL中包含表名的列表 json.dumps(list)',
  `plan_text`               LONGTEXT NULL COMMENT '执行计划文本',
  `plan_raw`                LONGTEXT NULL COMMENT '执行计划裸数据 json.dumps(list->(tuple))',
  `sql_data`                LONGTEXT NULL COMMENT '算法依赖的数据 json.dumps(dict)',
  `ai_result`               text NULL COMMENT 'AI结论 json.dumps(dict)',
  `message`                 text NOT NULL COMMENT '记录处理内容',
  `ai_error_code`           VARCHAR(128) NOT NULL COMMENT 'message',
  `ai_error_type`           VARCHAR(128) NOT NULL COMMENT 'error type',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_ai_sql_sequence` (`sequence`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


ALTER TABLE `dpaacc`.`ai_sr_sql_detail`
ADD COLUMN `ai_error_code` VARCHAR(128) NULL AFTER `message`,
ADD COLUMN `ai_error_type` VARCHAR(128) NULL AFTER `ai_error_code`;





-- AI SQLReview 项目, http请求日志记录
drop table `ai_sr_http_request_log`;
CREATE TABLE `ai_sr_http_request_log` (
  `id`                      int NOT NULL AUTO_INCREMENT,
  `created_by`              varchar(128) NOT NULL,
  `created_at`              datetime NOT NULL,
  `updated_by`              varchar(128) NOT NULL,
  `updated_at`              datetime NOT NULL,
  `ip`                      varchar(256) NOT NULL COMMENT '请求IP',   # nginx转发后, 会有多个IP
  `auth_data`               varchar(256) NULL COMMENT '请求的验证数据',
  `request_type`            varchar(32) NULL COMMENT '请求类型 POST, GET',
  `request_data`            text NULL COMMENT '请求数据',
  `response_data`           text NULL COMMENT '响应数据', # 异步请求无法回写

  PRIMARY KEY (`id`),
  KEY `idx_ai_sr_http_log_created_at` (`created_at`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- AI SQLReview 项目, 用户策略管理
drop table `ai_sr_profile_manage`;
CREATE TABLE `ai_sr_profile_manage` (
  `id`                      int NOT NULL AUTO_INCREMENT,
  `created_by`              varchar(128) NOT NULL,
  `created_at`              datetime NOT NULL,
  `updated_by`              varchar(128) NOT NULL,
  `updated_at`              datetime NOT NULL,
  `userid`                  varchar(128) NOT NULL COMMENT '用户ID',
  `tenant`                  varchar(128) NOT NULL COMMENT '租户',
  `profile_name`            varchar(128) NOT NULL COMMENT '策略名称',
  `db_type`                 varchar(128) NOT NULL COMMENT 'DB类型',
  `host`                    varchar(256) NULL COMMENT '主机地址',
  `port`                    varchar(256) NULL COMMENT '主机端口',
  `instance_name`           varchar(256) NULL COMMENT '实例名称 for Oracle',
  `username`                varchar(256) NULL COMMENT '连接用户名',
  `passwd`                  varchar(256) NULL COMMENT '连接密码',
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_ai_srpm_tenant_profilename` (`tenant`, `profile_name`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- 临时功能，记录租户与SCHEMA的映射关系，满足演示需要
CREATE TABLE `ai_sr_tenant_schema` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(128) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(128) NOT NULL,
  `updated_at` datetime NOT NULL,
  `userid` varchar(128) NOT NULL,
  `tenant` varchar(128) NOT NULL COMMENT '租户',
  `profile_name` varchar(128) NOT NULL COMMENT '策略名称',
  `db_type` varchar(128) NOT NULL COMMENT 'DB类型',
  `schema_name` varchar(128) NOT NULL COMMENT 'SCHEMA',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- AI 任务
CREATE TABLE `ai_sr_task` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(128) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(128) NOT NULL,
  `updated_at` datetime NOT NULL,
  `task_id` varchar(128) NOT NULL COMMENT '任务ID',
  `task_type` varchar(128) NOT NULL COMMENT '任务类型：SINGLE,GITREPOSITORY,SQLMAPFILE ......',
  `task_status` varchar(128) NOT NULL COMMENT '任务状态：INIT, PROCESSING, SUCCESS, FAILED ......',
  `task_message` text NULL COMMENT '任务消息',
  `comment` text NULL COMMENT '备注',
  `sql_total` varchar(128) NULL COMMENT 'SQL总数',
  `sql_need_optimize` varchar(128) NULL COMMENT '需要优化的SQL数',

  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- AI 任务SQL关系数据
CREATE TABLE `ai_sr_task_sql` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(128) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(128) NOT NULL,
  `updated_at` datetime NOT NULL,
  `task_id` varchar(128) NOT NULL COMMENT '任务ID',
  `task_type` varchar(128) NOT NULL COMMENT '任务类型：SINGLE,GITREPOSITORY,SQLMAPFILE ......',
  `sql_id_alias` varchar(128) NOT NULL COMMENT 'SQL_ID',
  `sql_sequence` varchar(128) NOT NULL COMMENT 'SQL SEQ FOR DETAIL',
  `db_type` varchar(128) NOT NULL COMMENT 'ORACLE | MYSQL ......',
  `schema_name` varchar(128) NOT NULL COMMENT 'SCHEMA名称',
  `sql_text` text NOT NULL COMMENT 'SQL 文本',
  `ref_id` text NULL COMMENT 'SQL 对应的关联ID，文件即FILE_ID',

  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_ai_sr_task_sql_sequence` (`sql_sequence`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
