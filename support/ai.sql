-- MySQL dump 10.13  Distrib 5.6.15, for Linux (x86_64)
--
-- Host: localhost    Database: dpaacc
-- ------------------------------------------------------
-- Server version	5.6.15-rel63.0-log

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `ai_sr_app_repository`
--

DROP TABLE IF EXISTS `ai_sr_app_repository`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ai_sr_app_repository` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime NOT NULL,
  `delete_mark` varchar(1) DEFAULT '1',
  `app_name` varchar(64) DEFAULT NULL,
  `repo_url` varchar(255) DEFAULT NULL,
  `is_auth` varchar(1) DEFAULT '1',
  `username` varchar(64) DEFAULT NULL,
  `password` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `app_name` (`app_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ai_sr_app_sqlmap`
--

DROP TABLE IF EXISTS `ai_sr_app_sqlmap`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ai_sr_app_sqlmap` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime NOT NULL,
  `review_request_id` bigint(20) NOT NULL COMMENT 'ai_sr_review_request id',
  `version` varchar(100) DEFAULT NULL COMMENT 'tag版本',
  `version_type` varchar(100) DEFAULT NULL COMMENT '类型, ADD-新增; MODIFY-修改',
  `filepath` varchar(200) DEFAULT NULL COMMENT '文件路径',
  `md5` varchar(100) DEFAULT NULL COMMENT '文件MD5',
  `content` longtext COMMENT '文件内容',
  `delete_mark` varchar(1) NOT NULL DEFAULT '1',
  PRIMARY KEY (`id`),
  KEY `review_request_id` (`review_request_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ai_sr_check_history`
--

DROP TABLE IF EXISTS `ai_sr_check_history`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ai_sr_check_history` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime NOT NULL,
  `delete_mark` varchar(1) DEFAULT NULL,
  `app_name` varchar(250) DEFAULT NULL,
  `tag` varchar(20) DEFAULT NULL,
  `task_id` varchar(100) DEFAULT NULL,
  `result` varchar(250) DEFAULT NULL,
  `msg` varchar(512) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ai_sr_http_request_log`
--

DROP TABLE IF EXISTS `ai_sr_http_request_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ai_sr_http_request_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(128) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(128) NOT NULL,
  `updated_at` datetime NOT NULL,
  `ip` varchar(256) NOT NULL COMMENT '请求IP',
  `auth_data` varchar(256) DEFAULT NULL COMMENT '请求的验证数据',
  `request_type` varchar(32) DEFAULT NULL COMMENT '请求类型 POST, GET',
  `request_data` text COMMENT '请求数据',
  `response_data` text COMMENT '响应数据',
  PRIMARY KEY (`id`),
  KEY `idx_ai_sr_http_log_created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ai_sr_profile_manage`
--

DROP TABLE IF EXISTS `ai_sr_profile_manage`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ai_sr_profile_manage` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(128) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(128) NOT NULL,
  `updated_at` datetime NOT NULL,
  `userid` varchar(128) NOT NULL,
  `tenant` varchar(128) NOT NULL COMMENT '租户',
  `profile_name` varchar(128) NOT NULL COMMENT '策略名称',
  `db_type` varchar(128) NOT NULL COMMENT 'DB类型',
  `host` varchar(256) DEFAULT NULL COMMENT '主机地址',
  `port` varchar(256) DEFAULT NULL COMMENT '主机端口',
  `instance_name` varchar(256) DEFAULT NULL COMMENT '实例名称 for Oracle',
  `username` varchar(256) DEFAULT NULL COMMENT '连接用户名',
  `passwd` varchar(256) DEFAULT NULL COMMENT '连接密码',
  `description` varchar(512) DEFAULT NULL,
  `delete_mark` varchar(1) NOT NULL DEFAULT '1',
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_ai_srpm_tenant_profilename` (`tenant`,`profile_name`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ai_sr_review_detail`
--

DROP TABLE IF EXISTS `ai_sr_review_detail`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ai_sr_review_detail` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime NOT NULL,
  `review_request_id` bigint(20) NOT NULL COMMENT 'ai_sr_review_request id',
  `namespace` varchar(200) NOT NULL COMMENT 'sql namespace',
  `sqlmap_id` varchar(200) NOT NULL COMMENT 'sql sqlmapid',
  `sql_modified_type` varchar(100) NOT NULL COMMENT 'SQL处理类型, ADD-新增, UPDATE-更新',
  `sql_old_text` longtext COMMENT '基准版本SQL',
  `sql_new_text` longtext NOT NULL COMMENT '新版本SQL',
  `plan_old_text` longtext COMMENT '基准版本计划文本',
  `plan_new_text` longtext COMMENT '新版本计划文本',
  `sqlmap_files` longtext COMMENT 'xml文件路径',
  `review_status` varchar(50) DEFAULT NULL COMMENT '评审状态 SUCCESS， FAIL',
  `review_comment` longtext COMMENT '评审备注',
  `sub_code` varchar(100) DEFAULT NULL,
  `hint_status` int(11) DEFAULT NULL COMMENT '是否使用了HINT',
  `white_list` varchar(10) DEFAULT NULL,
  `dynamic_sql` tinyint(4) DEFAULT '0' COMMENT '是否使用了动态拼接',
  `tip_code` varchar(100) DEFAULT NULL,
  `base_plan` varchar(10) DEFAULT NULL COMMENT '计划来源, T:测试,P:生产',
  `table_cnt` bigint(20) DEFAULT NULL COMMENT '表的数量',
  `table_names` varchar(2048) DEFAULT NULL COMMENT '表名内容, json.dumps(list)',
  `order_by` varchar(10) DEFAULT NULL COMMENT '是否使用了ORDER BY',
  `db_type` varchar(50) DEFAULT NULL COMMENT 'DB类型',
  `ai_result` varchar(128) DEFAULT NULL COMMENT 'AI结果',
  `sequence` varchar(128) DEFAULT NULL COMMENT 'sequence 与AI关联',
  `ai_recommend` text COMMENT 'SQL AI建议',
  `message` text COMMENT 'AI结果消息',
  `sql_correct` varchar(10) DEFAULT NULL COMMENT 'SQL语法是否正确',
  `dev_confirm` varchar(10) DEFAULT NULL COMMENT '开发是否确认',
  `schema_name` varchar(50) DEFAULT NULL COMMENT '对应的SCHEMA',
  `delete_mark` varchar(1) NOT NULL DEFAULT '1',
  `sql_md5` varchar(50) DEFAULT NULL COMMENT 'SQL MD5',
  PRIMARY KEY (`id`),
  UNIQUE KEY `UK_DETAIL_SEQUENCE` (`sequence`),
  KEY `review_request_id` (`review_request_id`),
  KEY `sqlmap` (`namespace`(191),`sqlmap_id`(191))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ai_sr_review_request`
--

DROP TABLE IF EXISTS `ai_sr_review_request`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ai_sr_review_request` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime NOT NULL,
  `task_id` varchar(128) NOT NULL COMMENT '任务ID',
  `app_name` varchar(50) DEFAULT NULL COMMENT '应用名称',
  `repo_path` varchar(1000) DEFAULT NULL COMMENT '应用路径',
  `review_type` varchar(100) NOT NULL COMMENT 'REVIEW类型：ALL全量, INC:增量',
  `review_status` varchar(100) NOT NULL COMMENT '状态; INIT-初始化, PARSING-分析中, PARSE FAILED- 分析失败, READY-就绪, REVIWING-评审中, SUCCESS-通过, FAIL-不通过',
  `older_version` varchar(100) DEFAULT NULL COMMENT '基准TAG',
  `newer_version` varchar(100) DEFAULT NULL COMMENT '新TAG',
  `comments` text COMMENT '备注',
  `all_sqlmap_md5sum` varchar(100) DEFAULT NULL COMMENT '所有XML的MD5',
  `allow_review` varchar(30) DEFAULT NULL,
  `allowed_dba` varchar(100) DEFAULT NULL,
  `old_request_id` int(11) DEFAULT NULL,
  `sql_cnt` int(11) DEFAULT NULL COMMENT '需要REVIEW的SQL总数',
  `delete_mark` varchar(1) NOT NULL DEFAULT '1',
  PRIMARY KEY (`id`),
  KEY `app_name` (`app_name`,`review_status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ai_sr_schema_table`
--

DROP TABLE IF EXISTS `ai_sr_schema_table`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ai_sr_schema_table` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(128) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(128) NOT NULL,
  `updated_at` datetime NOT NULL,
  `userid` varchar(128) NOT NULL,
  `tenant` varchar(128) NOT NULL COMMENT '租户标识',
  `schema_id` int(11) NOT NULL COMMENT 'ai_sr_tenant_schema id',
  `table_name` varchar(128) NOT NULL COMMENT 'table',
  `delete_mark` varchar(1) NOT NULL DEFAULT '1',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_user_ins_conf_utst` (`userid`,`tenant`,`schema_id`,`table_name`)
) ENGINE=InnoDB AUTO_INCREMENT=474 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ai_sr_sql_detail`
--

DROP TABLE IF EXISTS `ai_sr_sql_detail`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ai_sr_sql_detail` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(128) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(128) NOT NULL,
  `updated_at` datetime NOT NULL,
  `sequence` varchar(128) NOT NULL COMMENT '标识SQL唯一序列',
  `tenant_code` varchar(128) NOT NULL COMMENT '租户标识',
  `db_type` varchar(128) NOT NULL COMMENT 'ORACLE | MYSQL ......',
  `db_conn_url` varchar(512) DEFAULT NULL COMMENT 'DB 连接串',
  `schema_name` varchar(128) NOT NULL COMMENT 'SCHEMA名称',
  `sql_text` text NOT NULL COMMENT 'SQL 文本',
  `statement` varchar(128) DEFAULT NULL COMMENT 'SELECT | UPDATE | DELETE | INSERT ......',
  `dynamic_mosaicking` varchar(128) DEFAULT NULL,
  `table_names` varchar(4096) DEFAULT NULL COMMENT 'SQL中包含表名的列表 json.dumps(list)',
  `plan_text` longtext COMMENT '执行计划文本',
  `plan_raw` longtext COMMENT '执行计划裸数据 json.dumps(list->(tuple))',
  `sql_data` longtext COMMENT '算法依赖的数据 json.dumps(dict)',
  `ai_result` text COMMENT 'AI结论 json.dumps(dict)',
  `message` text NOT NULL COMMENT '记录处理内容',
  `ai_error_code` varchar(128) DEFAULT NULL,
  `ai_error_type` varchar(128) DEFAULT NULL,
  `ai_program_type` text COMMENT '不通过的分类信息',
  `opt_sql_text` text COMMENT '优化后的SQL',
  `opt_plan_text` text COMMENT '优化后的PLAN',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_ai_sql_sequence` (`sequence`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ai_sr_task`
--

DROP TABLE IF EXISTS `ai_sr_task`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ai_sr_task` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(128) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(128) NOT NULL,
  `updated_at` datetime NOT NULL,
  `tenant` varchar(128) DEFAULT NULL,
  `userid` varchar(128) DEFAULT NULL,
  `task_id` varchar(128) NOT NULL COMMENT '任务ID',
  `task_type` varchar(128) NOT NULL COMMENT '任务类型：SINGLE,GITREPOSITORY,SQLMAPFILE ......',
  `task_status` varchar(128) NOT NULL COMMENT '任务状态：INIT, PROCESSING, SUCCESS, FAILED ......',
  `task_message` text COMMENT '任务消息',
  `comment` text COMMENT '备注',
  `sql_total` varchar(128) DEFAULT NULL COMMENT 'SQL总数',
  `sql_need_optimize` varchar(128) DEFAULT NULL COMMENT '需要优化的SQL数',
  `delete_mark` varchar(1) NOT NULL DEFAULT '1',
  `app_name` varchar(128) DEFAULT NULL COMMENT '应用名',
  `base_tag` varchar(128) DEFAULT NULL COMMENT '基准版本TAG',
  `current_tag` varchar(128) DEFAULT NULL COMMENT '新版本TAG',
  `tag_review_result` varchar(32) DEFAULT NULL COMMENT 'TAG版本REVIEW结果',
  `valid` varchar(1) NOT NULL DEFAULT '1',
  PRIMARY KEY (`id`),
  KEY `IDX_AI_SR_TASK_TASK_ID` (`task_id`)
) ENGINE=InnoDB AUTO_INCREMENT=12 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ai_sr_task_sql`
--

DROP TABLE IF EXISTS `ai_sr_task_sql`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
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
  `plan_text` text COMMENT 'SQL PLAN',
  `ref_id` varchar(128) DEFAULT NULL COMMENT 'SQL 对应的关联ID，文件即FILE_ID',
  `review_status` varchar(32) DEFAULT NULL COMMENT '人工REVIEW结果',
  `ai_result` varchar(128) DEFAULT NULL COMMENT 'ai结果',
  `ai_recommend` text COMMENT 'ai建议',
  `ai_message` text COMMENT 'ai消息',
  `ai_error_code` varchar(128) DEFAULT NULL,
  `ai_error_type` varchar(128) DEFAULT NULL,
  `delete_mark` varchar(1) NOT NULL DEFAULT '1',
  `opt_sql_text` text COMMENT '优化后的SQL',
  `opt_plan_text` text COMMENT '优化后的PLAN',
  `ai_program_type` varchar(128) DEFAULT NULL COMMENT '不通过的分类信息',
  `sql_md5` varchar(32) DEFAULT NULL COMMENT 'SQL MD5',
  `valid` varchar(1) NOT NULL DEFAULT '1',
  `review_compare` varchar(32) DEFAULT NULL COMMENT '对比的SQL信息',
  `sql_review_suggest` text COMMENT 'DBA建议',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_ai_sr_task_sql_sequence` (`sql_sequence`),
  KEY `IDX_AI_SR_SQL_MD5` (`sql_md5`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ai_sr_tenant_schema`
--

DROP TABLE IF EXISTS `ai_sr_tenant_schema`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
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
  `delete_mark` varchar(1) NOT NULL DEFAULT '1',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=18 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ai_sr_user`
--

DROP TABLE IF EXISTS `ai_sr_user`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ai_sr_user` (
  `created_by` varchar(128) NOT NULL,
  `created_at` datetime(6) NOT NULL,
  `updated_by` varchar(128) NOT NULL,
  `updated_at` datetime(6) NOT NULL,
  `delete_mark` varchar(1) DEFAULT NULL,
  `userid` varchar(255) DEFAULT NULL,
  `tenant` varchar(255) DEFAULT NULL,
  `username` varchar(32) NOT NULL,
  `password` varchar(255) DEFAULT NULL,
  `department` varchar(255) DEFAULT NULL,
  `role` varchar(255) DEFAULT NULL,
  `email` varchar(255) DEFAULT NULL,
  `username_chn` varchar(255) DEFAULT NULL,
  `show_count` int(11) DEFAULT NULL COMMENT '显示统计开关',
  PRIMARY KEY (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `arch_part_process_log`
--

DROP TABLE IF EXISTS `arch_part_process_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `arch_part_process_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime(6) NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime(6) NOT NULL,
  `owner` varchar(30) NOT NULL,
  `tbl_name` varchar(200) NOT NULL,
  `part_name` varchar(200) NOT NULL,
  `step` varchar(200) DEFAULT NULL,
  `status` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=34687 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `arch_part_process_log_mysql`
--

DROP TABLE IF EXISTS `arch_part_process_log_mysql`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `arch_part_process_log_mysql` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime(6) NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime(6) NOT NULL,
  `owner` varchar(30) NOT NULL,
  `tbl_name` varchar(200) NOT NULL,
  `part_name` varchar(200) NOT NULL,
  `step` varchar(200) DEFAULT NULL,
  `status` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=34687 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `auth_group`
--

DROP TABLE IF EXISTS `auth_group`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `auth_group` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(80) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `auth_group_permissions`
--

DROP TABLE IF EXISTS `auth_group_permissions`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `auth_group_permissions` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `group_id` int(11) NOT NULL,
  `permission_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `auth_group_permissions_group_id_0cd325b0_uniq` (`group_id`,`permission_id`),
  KEY `auth_group_permissi_permission_id_84c5c92e_fk_auth_permission_id` (`permission_id`),
  CONSTRAINT `auth_group_permissions_group_id_b120cbf9_fk_auth_group_id` FOREIGN KEY (`group_id`) REFERENCES `auth_group` (`id`),
  CONSTRAINT `auth_group_permissi_permission_id_84c5c92e_fk_auth_permission_id` FOREIGN KEY (`permission_id`) REFERENCES `auth_permission` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `auth_permission`
--

DROP TABLE IF EXISTS `auth_permission`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `auth_permission` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `content_type_id` int(11) NOT NULL,
  `codename` varchar(100) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `auth_permission_content_type_id_01ab375a_uniq` (`content_type_id`,`codename`),
  CONSTRAINT `auth_permissi_content_type_id_2f476e4b_fk_django_content_type_id` FOREIGN KEY (`content_type_id`) REFERENCES `django_content_type` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `auth_user`
--

DROP TABLE IF EXISTS `auth_user`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `auth_user` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `password` varchar(128) NOT NULL,
  `last_login` datetime(6) DEFAULT NULL,
  `is_superuser` tinyint(1) NOT NULL,
  `username` varchar(150) NOT NULL,
  `first_name` varchar(30) NOT NULL,
  `last_name` varchar(30) NOT NULL,
  `email` varchar(254) NOT NULL,
  `is_staff` tinyint(1) NOT NULL,
  `is_active` tinyint(1) NOT NULL,
  `date_joined` datetime(6) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `username` (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `auth_user_groups`
--

DROP TABLE IF EXISTS `auth_user_groups`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `auth_user_groups` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `group_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `auth_user_groups_user_id_94350c0c_uniq` (`user_id`,`group_id`),
  KEY `auth_user_groups_group_id_97559544_fk_auth_group_id` (`group_id`),
  CONSTRAINT `auth_user_groups_group_id_97559544_fk_auth_group_id` FOREIGN KEY (`group_id`) REFERENCES `auth_group` (`id`),
  CONSTRAINT `auth_user_groups_user_id_6a12ed8b_fk_auth_user_id` FOREIGN KEY (`user_id`) REFERENCES `auth_user` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `auth_user_user_permissions`
--

DROP TABLE IF EXISTS `auth_user_user_permissions`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `auth_user_user_permissions` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `permission_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `auth_user_user_permissions_user_id_14a6b632_uniq` (`user_id`,`permission_id`),
  KEY `auth_user_user_perm_permission_id_1fbb5f2c_fk_auth_permission_id` (`permission_id`),
  CONSTRAINT `auth_user_user_permissions_user_id_a95ead1b_fk_auth_user_id` FOREIGN KEY (`user_id`) REFERENCES `auth_user` (`id`),
  CONSTRAINT `auth_user_user_perm_permission_id_1fbb5f2c_fk_auth_permission_id` FOREIGN KEY (`permission_id`) REFERENCES `auth_permission` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `authtoken_token`
--

DROP TABLE IF EXISTS `authtoken_token`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `authtoken_token` (
  `key` varchar(40) NOT NULL,
  `created` datetime(6) NOT NULL,
  `user_id` varchar(32) NOT NULL,
  PRIMARY KEY (`key`),
  UNIQUE KEY `user_id` (`user_id`),
  CONSTRAINT `authtoken_token_user_id_35299eff_fk_ai_sr_user_username` FOREIGN KEY (`user_id`) REFERENCES `ai_sr_user` (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bd_sql_interaction`
--

DROP TABLE IF EXISTS `bd_sql_interaction`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bd_sql_interaction` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime(6) NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime(6) NOT NULL,
  `sql_unique` varchar(128) DEFAULT NULL,
  `sql_text` longtext,
  `db_type` varchar(128) DEFAULT NULL,
  `sql_instance_name` varchar(128) DEFAULT NULL,
  `sql_schema_name` varchar(128) DEFAULT NULL,
  `sql_bettle_id` bigint(20) DEFAULT NULL,
  `sql_file_id` bigint(20) DEFAULT NULL,
  `sql_piece_id` bigint(20) DEFAULT NULL,
  `interact_type` varchar(128) DEFAULT NULL,
  `interact_send_content` longtext,
  `sql_usage` varchar(128) DEFAULT NULL,
  `sql_schedule_exec_time` datetime DEFAULT NULL,
  `sql_send_time` datetime DEFAULT NULL,
  `sql_recv_time` datetime DEFAULT NULL,
  `sql_status` varchar(128) DEFAULT NULL,
  `interact_recv_content` longtext,
  `sql_session_connect_duation` int(11) DEFAULT NULL,
  `sql_duration` int(11) DEFAULT NULL,
  `sql_effect_row` int(11) DEFAULT NULL,
  `sql_exec_result` longtext,
  PRIMARY KEY (`id`),
  UNIQUE KEY `sql_unique` (`sql_unique`)
) ENGINE=InnoDB AUTO_INCREMENT=94699 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bd_sql_publish`
--

DROP TABLE IF EXISTS `bd_sql_publish`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bd_sql_publish` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime(6) NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime(6) NOT NULL,
  `sql_unique` varchar(128) DEFAULT NULL,
  `sql_text` longtext,
  `db_type` varchar(128) DEFAULT NULL,
  `sql_instance_name` varchar(128) DEFAULT NULL,
  `sql_schema_name` varchar(128) DEFAULT NULL,
  `sql_bettle_id` bigint(20) DEFAULT NULL,
  `sql_file_id` bigint(20) DEFAULT NULL,
  `sql_piece_id` bigint(20) DEFAULT NULL,
  `interact_type` varchar(128) DEFAULT NULL,
  `interact_send_content` longtext,
  `sql_usage` varchar(128) DEFAULT NULL,
  `sql_schedule_exec_time` datetime DEFAULT NULL,
  `sql_send_time` datetime DEFAULT NULL,
  `sql_recv_time` datetime DEFAULT NULL,
  `sql_status` varchar(128) DEFAULT NULL,
  `interact_recv_content` longtext,
  `sql_session_connect_duation` int(11) DEFAULT NULL,
  `sql_duration` int(11) DEFAULT NULL,
  `sql_effect_row` int(11) DEFAULT NULL,
  `sql_exec_result` longtext,
  PRIMARY KEY (`id`),
  UNIQUE KEY `sql_unique` (`sql_unique`)
) ENGINE=InnoDB AUTO_INCREMENT=56 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `celery_request_log`
--

DROP TABLE IF EXISTS `celery_request_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `celery_request_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime(6) NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime(6) NOT NULL,
  `uuid` varchar(64) NOT NULL,
  `itsm_id` varchar(100) DEFAULT NULL,
  `status` varchar(64) NOT NULL,
  `request_data` longtext,
  `comments` longtext,
  PRIMARY KEY (`id`),
  UNIQUE KEY `itsm_id` (`itsm_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `db_instance`
--

DROP TABLE IF EXISTS `db_instance`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `db_instance` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime NOT NULL,
  `name` varchar(50) NOT NULL,
  `usage` varchar(10) NOT NULL,
  `db_type` varchar(10) NOT NULL,
  `url` varchar(100) NOT NULL,
  `username` varchar(100) NOT NULL,
  `password` varchar(100) NOT NULL,
  `dba_username` varchar(100) DEFAULT NULL,
  `dba_password` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `db_schema_group`
--

DROP TABLE IF EXISTS `db_schema_group`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `db_schema_group` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime NOT NULL,
  `name` varchar(50) NOT NULL,
  `data_user` varchar(50) NOT NULL,
  `opr_user` varchar(50) NOT NULL,
  `rnd_user` varchar(50) NOT NULL,
  `instance_type` varchar(16) DEFAULT NULL,
  `is_managing` tinyint(1) DEFAULT NULL,
  `data_user_password` varchar(50) DEFAULT NULL,
  `zone` varchar(50) DEFAULT NULL,
  `area` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_db_schema_group_name_ins_type` (`name`,`instance_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dba_tab_col`
--

DROP TABLE IF EXISTS `dba_tab_col`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dba_tab_col` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime NOT NULL,
  `schema_group_id` bigint(20) NOT NULL,
  `instance_id` bigint(20) DEFAULT NULL,
  `table_id` bigint(20) NOT NULL,
  `source_column_id` bigint(20) DEFAULT NULL,
  `name` varchar(100) NOT NULL,
  `col_type` varchar(200) NOT NULL,
  `col_null` varchar(20) NOT NULL,
  `default_value` varchar(100) DEFAULT NULL,
  `comments` varchar(4000) DEFAULT NULL,
  `is_new` tinyint(1) NOT NULL,
  `version` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `source_column_id` (`source_column_id`),
  KEY `table_id` (`table_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dba_table`
--

DROP TABLE IF EXISTS `dba_table`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dba_table` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime NOT NULL,
  `schema_group_id` bigint(20) NOT NULL,
  `instance_id` bigint(20) DEFAULT NULL,
  `source_table_id` bigint(20) DEFAULT NULL,
  `name` varchar(64) DEFAULT NULL,
  `comments` varchar(4000) DEFAULT NULL,
  `rows_per_month` bigint(20) NOT NULL,
  `usage` varchar(10) NOT NULL,
  `frequency` varchar(10) NOT NULL,
  `locked_by_proj_id` bigint(20) NOT NULL,
  `version` bigint(20) NOT NULL,
  `is_new` tinyint(1) NOT NULL,
  `is_dropped` tinyint(1) NOT NULL,
  `sequence_created` tinyint(1) DEFAULT NULL,
  `tab_synonym_created` tinyint(1) DEFAULT NULL,
  `seq_synonym_created` tinyint(1) DEFAULT NULL,
  `opr_granted` tinyint(1) DEFAULT NULL,
  `rnd_granted` tinyint(1) DEFAULT NULL,
  `seq_opr_granted` tinyint(1) DEFAULT NULL,
  `sequence_name` varchar(30) DEFAULT NULL,
  `partition_methord` varchar(30) DEFAULT NULL,
  `partition_key_col` varchar(30) DEFAULT NULL,
  `partition_interval` varchar(128) DEFAULT NULL,
  `partition_keep_count` int(11) DEFAULT NULL,
  `is_split` tinyint(1) DEFAULT NULL,
  `erase_o` varchar(10) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `source_table_id` (`source_table_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dev_table`
--

DROP TABLE IF EXISTS `dev_table`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dev_table` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime NOT NULL,
  `schema_group_id` bigint(20) NOT NULL,
  `instance_id` bigint(20) DEFAULT NULL,
  `source_table_id` bigint(20) DEFAULT NULL,
  `name` varchar(64) DEFAULT NULL,
  `comments` varchar(4000) DEFAULT NULL,
  `rows_per_month` bigint(20) NOT NULL,
  `usage` varchar(10) NOT NULL,
  `frequency` varchar(10) NOT NULL,
  `locked_by_proj_id` bigint(20) NOT NULL,
  `version` bigint(20) NOT NULL,
  `is_new` tinyint(1) NOT NULL,
  `is_dropped` tinyint(1) NOT NULL,
  `sequence_created` tinyint(1) DEFAULT NULL,
  `tab_synonym_created` tinyint(1) DEFAULT NULL,
  `seq_synonym_created` tinyint(1) DEFAULT NULL,
  `opr_granted` tinyint(1) DEFAULT NULL,
  `rnd_granted` tinyint(1) DEFAULT NULL,
  `seq_opr_granted` tinyint(1) DEFAULT NULL,
  `sequence_name` varchar(30) DEFAULT NULL,
  `partition_methord` varchar(30) DEFAULT NULL,
  `partition_key_col` varchar(30) DEFAULT NULL,
  `partition_interval` varchar(128) DEFAULT NULL,
  `partition_keep_count` int(11) DEFAULT NULL,
  `is_split` tinyint(1) DEFAULT NULL,
  `erase_o` varchar(10) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`),
  UNIQUE KEY `source_table_id` (`source_table_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `django_admin_log`
--

DROP TABLE IF EXISTS `django_admin_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `django_admin_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `action_time` datetime(6) NOT NULL,
  `object_id` longtext,
  `object_repr` varchar(200) NOT NULL,
  `action_flag` smallint(5) unsigned NOT NULL,
  `change_message` longtext NOT NULL,
  `content_type_id` int(11) DEFAULT NULL,
  `user_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `django_admin__content_type_id_c4bce8eb_fk_django_content_type_id` (`content_type_id`),
  KEY `django_admin_log_user_id_c564eba6_fk_auth_user_id` (`user_id`),
  CONSTRAINT `django_admin_log_user_id_c564eba6_fk_auth_user_id` FOREIGN KEY (`user_id`) REFERENCES `auth_user` (`id`),
  CONSTRAINT `django_admin__content_type_id_c4bce8eb_fk_django_content_type_id` FOREIGN KEY (`content_type_id`) REFERENCES `django_content_type` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `django_celery_beat_crontabschedule`
--

DROP TABLE IF EXISTS `django_celery_beat_crontabschedule`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `django_celery_beat_crontabschedule` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `minute` varchar(64) NOT NULL,
  `hour` varchar(64) NOT NULL,
  `day_of_week` varchar(64) NOT NULL,
  `day_of_month` varchar(64) NOT NULL,
  `month_of_year` varchar(64) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `django_celery_beat_intervalschedule`
--

DROP TABLE IF EXISTS `django_celery_beat_intervalschedule`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `django_celery_beat_intervalschedule` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `every` int(11) NOT NULL,
  `period` varchar(24) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `django_celery_beat_periodictask`
--

DROP TABLE IF EXISTS `django_celery_beat_periodictask`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `django_celery_beat_periodictask` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(200) NOT NULL,
  `task` varchar(200) NOT NULL,
  `args` longtext NOT NULL,
  `kwargs` longtext NOT NULL,
  `queue` varchar(200) DEFAULT NULL,
  `exchange` varchar(200) DEFAULT NULL,
  `routing_key` varchar(200) DEFAULT NULL,
  `expires` datetime(6) DEFAULT NULL,
  `enabled` tinyint(1) NOT NULL,
  `last_run_at` datetime(6) DEFAULT NULL,
  `total_run_count` int(10) unsigned NOT NULL,
  `date_changed` datetime(6) NOT NULL,
  `description` longtext NOT NULL,
  `crontab_id` int(11) DEFAULT NULL,
  `interval_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`),
  KEY `dja_crontab_id_d3cba168_fk_django_celery_beat_crontabschedule_id` (`crontab_id`),
  KEY `d_interval_id_a8ca27da_fk_django_celery_beat_intervalschedule_id` (`interval_id`),
  CONSTRAINT `dja_crontab_id_d3cba168_fk_django_celery_beat_crontabschedule_id` FOREIGN KEY (`crontab_id`) REFERENCES `django_celery_beat_crontabschedule` (`id`),
  CONSTRAINT `d_interval_id_a8ca27da_fk_django_celery_beat_intervalschedule_id` FOREIGN KEY (`interval_id`) REFERENCES `django_celery_beat_intervalschedule` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `django_celery_beat_periodictasks`
--

DROP TABLE IF EXISTS `django_celery_beat_periodictasks`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `django_celery_beat_periodictasks` (
  `ident` smallint(6) NOT NULL,
  `last_update` datetime(6) NOT NULL,
  PRIMARY KEY (`ident`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `django_celery_results_taskresult`
--

DROP TABLE IF EXISTS `django_celery_results_taskresult`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `django_celery_results_taskresult` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `task_id` varchar(255) NOT NULL,
  `status` varchar(50) NOT NULL,
  `content_type` varchar(128) NOT NULL,
  `content_encoding` varchar(64) NOT NULL,
  `result` longtext,
  `date_done` datetime(6) NOT NULL,
  `traceback` longtext,
  `hidden` tinyint(1) NOT NULL,
  `meta` longtext,
  PRIMARY KEY (`id`),
  UNIQUE KEY `task_id` (`task_id`),
  KEY `django_celery_results_taskresult_662f707d` (`hidden`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `django_content_type`
--

DROP TABLE IF EXISTS `django_content_type`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `django_content_type` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `app_label` varchar(100) NOT NULL,
  `model` varchar(100) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `django_content_type_app_label_76bd3d3b_uniq` (`app_label`,`model`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `django_migrations`
--

DROP TABLE IF EXISTS `django_migrations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `django_migrations` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `app` varchar(255) NOT NULL,
  `name` varchar(255) NOT NULL,
  `applied` datetime(6) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `django_session`
--

DROP TABLE IF EXISTS `django_session`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `django_session` (
  `session_key` varchar(40) NOT NULL,
  `session_data` longtext NOT NULL,
  `expire_date` datetime(6) NOT NULL,
  PRIMARY KEY (`session_key`),
  KEY `django_session_de54fa62` (`expire_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_ai_monitor`
--

DROP TABLE IF EXISTS `dpaa_ai_monitor`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_ai_monitor` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(128) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(128) NOT NULL,
  `updated_at` datetime NOT NULL,
  `db_type` varchar(128) NOT NULL COMMENT 'DB类型 ORACLE MYSQL REDIS',
  `instance_name` varchar(128) DEFAULT NULL COMMENT '实例信息, ORACLE-实例名, MYSQL-MYSQL+端口, REDIS-REDIS+PORT',
  `db_primary` varchar(512) DEFAULT NULL COMMENT '主库信息',
  `db_slave1` varchar(512) DEFAULT NULL COMMENT '第一备',
  `db_slave2` varchar(512) DEFAULT NULL COMMENT '第二备',
  `db_snapshot` varchar(512) DEFAULT NULL COMMENT 'SNAPSHOT',
  `db_slaves` varchar(4000) DEFAULT NULL COMMENT '备库列表',
  `ai_target` varchar(512) DEFAULT NULL COMMENT 'AI切换目标',
  `check_time` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `dpaa_aimonitor_checktime` (`check_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_api_user_key`
--

DROP TABLE IF EXISTS `dpaa_api_user_key`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_api_user_key` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime NOT NULL,
  `username` varchar(128) NOT NULL,
  `ip` varchar(30) NOT NULL,
  `secret_key` varchar(128) NOT NULL,
  `comments` varchar(4000) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `user_id` (`username`,`ip`)
) ENGINE=InnoDB AUTO_INCREMENT=18 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_check_config`
--

DROP TABLE IF EXISTS `dpaa_check_config`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_check_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(128) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(128) NOT NULL,
  `updated_at` datetime NOT NULL,
  `check_type` varchar(128) NOT NULL COMMENT '检查类型',
  `item` varchar(128) NOT NULL COMMENT 'item title',
  `item_type` varchar(128) DEFAULT NULL COMMENT 'item 类型, 标识业务类型',
  `item_key` varchar(128) DEFAULT NULL COMMENT 'item key',
  `static_check` int(11) NOT NULL COMMENT '静态值检查开关 0:CLOSE 1:OPEN',
  `static_value_type` varchar(128) DEFAULT NULL COMMENT '结果类型, returncode, number, string, boolean ',
  `command_static_type` varchar(128) DEFAULT NULL COMMENT 'command类型, [OS PLSQL MYSQL CACHE_COMPARE]',
  `command_static` text COMMENT '静态值检查时执行的指令,一般是文件值, 包括OS COMMAND, sql 等',
  `memory_check` int(11) NOT NULL COMMENT '动态值检查开关 0:CLOSE 1:OPEN',
  `memory_value_type` varchar(128) DEFAULT NULL COMMENT '结果类型, returncode, number, string, boolean ',
  `command_memory_type` varchar(128) DEFAULT NULL COMMENT 'command类型, [OS PLSQL MYSQL CACHE_COMPARE]',
  `command_memory` text COMMENT '动态检查时执行的指令,内存值, 包括OS COMMAND, sql 等',
  `ops_expression` varchar(128) DEFAULT NULL COMMENT '操作表达式, [ > | < | >= | <= | == | range | in | include | ignore ]',
  `expect_value` text COMMENT '预期的值, 包含范围',
  `priority` int(11) NOT NULL COMMENT '优先级',
  `item_is_need_check` int(11) NOT NULL COMMENT '检查开关 0:CLOSE 1:OPEN',
  `comment` varchar(4000) DEFAULT NULL COMMENT '备注',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=43 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_check_monitor`
--

DROP TABLE IF EXISTS `dpaa_check_monitor`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_check_monitor` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(128) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(128) NOT NULL,
  `updated_at` datetime NOT NULL,
  `ip` varchar(128) NOT NULL,
  `check_type` varchar(128) NOT NULL COMMENT '检查类型',
  `instance_name` varchar(128) NOT NULL,
  `config_item_id` int(11) NOT NULL COMMENT 'dpaa_check_config.id',
  `item` varchar(128) NOT NULL COMMENT 'item title',
  `item_type` varchar(128) DEFAULT NULL COMMENT 'item 类型, 标识业务类型',
  `item_key` varchar(128) DEFAULT NULL COMMENT 'item key',
  `expect_value` text COMMENT '预期的值, 包含范围',
  `static_value` text COMMENT '静态值, 实际获取',
  `static_result` varchar(128) DEFAULT NULL COMMENT '静态值, 实际获取',
  `memory_value` text COMMENT '动态值, 实际获取',
  `memory_result` varchar(128) DEFAULT NULL COMMENT '动态值, 实际获取',
  `checkpoint` varchar(128) NOT NULL COMMENT '检查点, 镜像',
  `comment` varchar(4000) DEFAULT NULL COMMENT '备注',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_dpaa_check_monitor_ipinsid` (`checkpoint`,`ip`,`instance_name`,`config_item_id`)
) ENGINE=InnoDB AUTO_INCREMENT=71 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_data_switch`
--

DROP TABLE IF EXISTS `dpaa_data_switch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_data_switch` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime NOT NULL,
  `switch_key` varchar(100) NOT NULL,
  `switch_value` int(11) DEFAULT NULL,
  `comments` longtext,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_db_pre_config`
--

DROP TABLE IF EXISTS `dpaa_db_pre_config`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_db_pre_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(128) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(128) NOT NULL,
  `updated_at` datetime NOT NULL,
  `db_config_type` varchar(128) NOT NULL COMMENT '配置类型',
  `db_key` varchar(128) NOT NULL COMMENT '配置关键字',
  `db_value` varchar(512) NOT NULL COMMENT '配置值',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_dmi_name` (`db_config_type`,`db_key`)
) ENGINE=InnoDB AUTO_INCREMENT=67 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_dba_info`
--

DROP TABLE IF EXISTS `dpaa_dba_info`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_dba_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime(6) NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime(6) NOT NULL,
  `um` varchar(128) DEFAULT NULL,
  `mobile` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_debug_log`
--

DROP TABLE IF EXISTS `dpaa_debug_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_debug_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(128) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(128) NOT NULL,
  `updated_at` datetime NOT NULL,
  `operation` varchar(1024) DEFAULT NULL COMMENT '操作',
  `content` varchar(4000) DEFAULT NULL COMMENT '内容',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4586 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_dns_opt_log`
--

DROP TABLE IF EXISTS `dpaa_dns_opt_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_dns_opt_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime NOT NULL,
  `plan_id` varchar(128) NOT NULL,
  `dns_data` longtext,
  `start_time` datetime DEFAULT NULL,
  `end_time` datetime DEFAULT NULL,
  `modify_result` varchar(128) DEFAULT NULL,
  `comments` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_hadoop_manage`
--

DROP TABLE IF EXISTS `dpaa_hadoop_manage`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_hadoop_manage` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime(6) NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime(6) NOT NULL,
  `hadoop_type` varchar(128) NOT NULL COMMENT 'hadoop标识, 用于区分集群',
  `description` varchar(4000) DEFAULT NULL,
  `hive_host` varchar(128) DEFAULT NULL COMMENT 'hiveserver2 ip',
  `hive_port` int(11) DEFAULT NULL COMMENT 'hiveserver2 port',
  `hive_queue` varchar(128) DEFAULT NULL,
  `hive_auth` varchar(128) DEFAULT NULL,
  `hive_kerberos_service_name` varchar(128) DEFAULT NULL,
  `hive_ldap_username` varchar(128) DEFAULT NULL,
  `hive_ldap_password` varchar(128) DEFAULT NULL,
  `impala_host` varchar(128) DEFAULT NULL COMMENT 'impala ip',
  `impala_port` int(11) DEFAULT NULL COMMENT 'impala port',
  `impala_auth` varchar(128) DEFAULT NULL,
  `impala_kerberos_service_name` varchar(128) DEFAULT NULL,
  `impala_ldap_username` varchar(128) DEFAULT NULL,
  `impala_ldap_password` varchar(128) DEFAULT NULL,
  `comment` longtext,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_dpaahadoopmanage_hadooptype` (`hadoop_type`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_instance_conn_info`
--

DROP TABLE IF EXISTS `dpaa_instance_conn_info`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_instance_conn_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime(6) NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime(6) NOT NULL,
  `instance_name` varchar(30) NOT NULL,
  `instance_type` varchar(30) NOT NULL,
  `master_ip` varchar(128) DEFAULT NULL,
  `master_port` varchar(128) DEFAULT NULL,
  `master_username` varchar(128) DEFAULT NULL,
  `master_passwd` varchar(128) DEFAULT NULL,
  `slave_ip` varchar(128) DEFAULT NULL,
  `slave_port` varchar(128) DEFAULT NULL,
  `slave_username` varchar(128) DEFAULT NULL,
  `slave_passwd` varchar(128) DEFAULT NULL,
  `snap_ip` varchar(128) DEFAULT NULL,
  `snap_port` varchar(128) DEFAULT NULL,
  `snap_username` varchar(128) DEFAULT NULL,
  `snap_passwd` varchar(128) DEFAULT NULL,
  `comment` varchar(4000) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_dpaainsconninfo_insname` (`instance_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_instance_logic`
--

DROP TABLE IF EXISTS `dpaa_instance_logic`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_instance_logic` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(128) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(128) NOT NULL,
  `updated_at` datetime NOT NULL,
  `instance_name` varchar(128) NOT NULL COMMENT '实例名',
  `db_type` varchar(128) NOT NULL COMMENT '数据库类型',
  `description` varchar(2048) DEFAULT NULL COMMENT '业务描述',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_dil_instance` (`instance_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_instance_runtime`
--

DROP TABLE IF EXISTS `dpaa_instance_runtime`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_instance_runtime` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(128) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(128) NOT NULL,
  `updated_at` datetime NOT NULL,
  `instance_id` int(11) NOT NULL COMMENT '实例名',
  `sessions` int(11) DEFAULT NULL COMMENT '业务SESSION连接数量',
  `check_time` datetime NOT NULL COMMENT '自动收集时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_dir_instanceid` (`instance_id`)
) ENGINE=InnoDB AUTO_INCREMENT=342 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_instance_schema_rel`
--

DROP TABLE IF EXISTS `dpaa_instance_schema_rel`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_instance_schema_rel` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime(6) NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime(6) NOT NULL,
  `instance_name` varchar(30) NOT NULL,
  `instance_type` varchar(30) NOT NULL,
  `schema_name` varchar(128) NOT NULL,
  `comment` varchar(4000) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_dpaainsschrel_schname` (`schema_name`,`instance_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_ip_manage`
--

DROP TABLE IF EXISTS `dpaa_ip_manage`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_ip_manage` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime(6) NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime(6) NOT NULL,
  `ip_address` varchar(128) DEFAULT NULL,
  `room` varchar(128) DEFAULT NULL,
  `dbtype` varchar(512) DEFAULT NULL,
  `status` int(11) DEFAULT NULL,
  `comment` varchar(4000) DEFAULT NULL,
  `used` int(11) DEFAULT NULL,
  `check_time` datetime DEFAULT NULL,
  `is_auth` varchar(1) DEFAULT NULL COMMENT 'ip是否验证设备, 验证设备不参与SCHEMA逻辑采集; 1:True, 0:FALSE',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_dpaa_ip_manage_ip` (`ip_address`)
) ENGINE=InnoDB AUTO_INCREMENT=298 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_message_resource_change`
--

DROP TABLE IF EXISTS `dpaa_message_resource_change`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_message_resource_change` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(128) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(128) NOT NULL,
  `updated_at` datetime NOT NULL,
  `meta_type` varchar(128) NOT NULL COMMENT '信息类型 OS | DATABASE | INSTANCE',
  `meta_key` varchar(128) NOT NULL,
  `change_type` varchar(128) NOT NULL COMMENT '信息变更方式 ONLINE | OFFLINE | CHANGE ',
  `change_desc` varchar(4000) DEFAULT NULL COMMENT '信息变更描述',
  `is_notify` int(11) NOT NULL COMMENT '是否通知标识',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=47808 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_meta_column`
--

DROP TABLE IF EXISTS `dpaa_meta_column`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_meta_column` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(128) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(128) NOT NULL,
  `updated_at` datetime NOT NULL,
  `name` varchar(128) NOT NULL COMMENT 'COLUMN名称',
  `table_id` int(11) NOT NULL COMMENT 'TABLE',
  `col_comment` varchar(1024) DEFAULT NULL COMMENT 'column comments',
  `col_type` varchar(128) DEFAULT NULL COMMENT 'column type',
  `col_is_null` varchar(128) DEFAULT NULL COMMENT '是否可以NULL',
  `col_default_value` varchar(512) DEFAULT NULL COMMENT 'column default value',
  `check_time` datetime NOT NULL COMMENT '自动收集时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_dmcol_name_tabid` (`name`,`table_id`)
) ENGINE=InnoDB AUTO_INCREMENT=335532 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_meta_database`
--

DROP TABLE IF EXISTS `dpaa_meta_database`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_meta_database` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(128) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(128) NOT NULL,
  `updated_at` datetime NOT NULL,
  `server_id` int(11) NOT NULL,
  `db_type` varchar(128) NOT NULL COMMENT 'DB类型 ORACLE MYSQL REDIS',
  `ins_info` varchar(128) DEFAULT NULL COMMENT '实例信息, ORACLE-实例名, MYSQL-MYSQL+端口, REDIS-REDIS+PORT',
  `common_port` varchar(512) DEFAULT NULL COMMENT '公共端口',
  `port_group` varchar(512) DEFAULT NULL COMMENT '端口集',
  `ora_listener` varchar(512) DEFAULT NULL COMMENT '监听端口名称, 与端口配对',
  `db_used` int(11) DEFAULT NULL COMMENT 'database是否可用',
  `comment` varchar(4000) DEFAULT NULL COMMENT '备注',
  `check_time` datetime NOT NULL COMMENT '自动收集时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_dmd_serid_dbtype_ins` (`server_id`,`db_type`,`ins_info`)
) ENGINE=InnoDB AUTO_INCREMENT=412 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_meta_database_extend`
--

DROP TABLE IF EXISTS `dpaa_meta_database_extend`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_meta_database_extend` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(128) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(128) NOT NULL,
  `updated_at` datetime NOT NULL,
  `db_type` varchar(128) NOT NULL COMMENT 'DB类型 ORACLE MYSQL REDIS',
  `instance_name` varchar(128) DEFAULT NULL COMMENT '实例信息, ORACLE-实例名, MYSQL-MYSQL+端口, REDIS-REDIS+PORT',
  `service_content` varchar(512) DEFAULT NULL COMMENT '业务内容',
  `service_usage` varchar(512) DEFAULT NULL COMMENT '使用范围',
  `db_desc` text COMMENT '数据库描述',
  `sys_desc` text COMMENT '系统描述',
  `dns` text COMMENT 'DNS - json string',
  `comment` varchar(4000) DEFAULT NULL COMMENT '备注',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_dmd_extend` (`db_type`,`instance_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_meta_except`
--

DROP TABLE IF EXISTS `dpaa_meta_except`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_meta_except` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime(6) NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime(6) NOT NULL,
  `ip` varchar(32) NOT NULL COMMENT '排除的IP',
  `db_type` varchar(32) NOT NULL COMMENT 'DB类型',
  `port` varchar(16) DEFAULT NULL COMMENT 'db端口, mysql识别',
  `instance_name` varchar(32) DEFAULT NULL COMMENT '实例名 oracle识别',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_meta_index`
--

DROP TABLE IF EXISTS `dpaa_meta_index`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_meta_index` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(128) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(128) NOT NULL,
  `updated_at` datetime NOT NULL,
  `name` varchar(128) NOT NULL COMMENT 'INDEX名称',
  `table_id` int(11) NOT NULL COMMENT 'TABLE',
  `ind_col` varchar(1024) DEFAULT NULL COMMENT 'index column list',
  `constraint_name` varchar(128) DEFAULT NULL COMMENT 'constraint name',
  `constraint_type` varchar(128) DEFAULT NULL COMMENT 'constraint type',
  `partitioned` varchar(128) DEFAULT NULL COMMENT 'is partition',
  `md5` varchar(128) DEFAULT NULL COMMENT 'md5',
  `check_time` datetime NOT NULL COMMENT '自动收集时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_dmind_name_tabid` (`name`,`table_id`)
) ENGINE=InnoDB AUTO_INCREMENT=29982 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_meta_instance`
--

DROP TABLE IF EXISTS `dpaa_meta_instance`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_meta_instance` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(128) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(128) NOT NULL,
  `updated_at` datetime NOT NULL,
  `database_id` int(11) NOT NULL,
  `instance_name` varchar(128) DEFAULT NULL COMMENT '实例名称',
  `instance_type` varchar(128) DEFAULT NULL COMMENT '类型, ORACLE MYSQL REDIS',
  `open_mode` varchar(128) DEFAULT NULL COMMENT 'OPEN MODE',
  `db_role` varchar(128) DEFAULT NULL COMMENT 'DATABASE_ROLE',
  `db_unique_name` varchar(128) DEFAULT NULL COMMENT 'DB唯一标识',
  `db_version` varchar(128) DEFAULT NULL COMMENT 'VERSION',
  `role` varchar(128) DEFAULT NULL COMMENT 'PRIMARY,SLAVE,SLAVE1,SLAVE2 ... , SNAPSHOT',
  `log_ref_dbid` varchar(128) DEFAULT NULL COMMENT '日志传输关系,ORACLE 指定子节点DBID, MYSQL指定父节点DBID',
  `mha_server` varchar(512) DEFAULT NULL COMMENT 'mha manager server',
  `mha_file` varchar(512) DEFAULT NULL COMMENT 'mha file name',
  `mysql_vip` varchar(128) DEFAULT NULL,
  `mysql_vip_dnsname` varchar(4000) DEFAULT NULL,
  `sentinel_group` varchar(512) DEFAULT NULL,
  `redis_cache_name` varchar(512) DEFAULT NULL,
  `comment` varchar(4000) DEFAULT NULL COMMENT '备注',
  `check_time` datetime NOT NULL COMMENT '自动收集时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_dmi_name` (`database_id`)
) ENGINE=InnoDB AUTO_INCREMENT=367 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_meta_instance_0523`
--

DROP TABLE IF EXISTS `dpaa_meta_instance_0523`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_meta_instance_0523` (
  `id` int(11) NOT NULL DEFAULT '0',
  `created_by` varchar(128) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(128) NOT NULL,
  `updated_at` datetime NOT NULL,
  `database_id` int(11) NOT NULL,
  `instance_name` varchar(128) DEFAULT NULL COMMENT '实例名称',
  `instance_type` varchar(128) DEFAULT NULL COMMENT '类型, ORACLE MYSQL REDIS',
  `open_mode` varchar(128) DEFAULT NULL COMMENT 'OPEN MODE',
  `db_role` varchar(128) DEFAULT NULL COMMENT 'DATABASE_ROLE',
  `db_unique_name` varchar(128) DEFAULT NULL COMMENT 'DB唯一标识',
  `db_version` varchar(128) DEFAULT NULL COMMENT 'VERSION',
  `role` varchar(128) DEFAULT NULL COMMENT 'PRIMARY,SLAVE,SLAVE1,SLAVE2 ... , SNAPSHOT',
  `log_ref_dbid` varchar(128) DEFAULT NULL COMMENT '日志传输关系,ORACLE 指定子节点DBID, MYSQL指定父节点DBID',
  `mysql_vip` varchar(128) DEFAULT NULL,
  `mysql_vip_dnsname` varchar(4000) DEFAULT NULL,
  `sentinel_group` varchar(512) DEFAULT NULL,
  `comment` varchar(4000) DEFAULT NULL COMMENT '备注',
  `check_time` datetime NOT NULL COMMENT '自动收集时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_meta_instance_app_relation`
--

DROP TABLE IF EXISTS `dpaa_meta_instance_app_relation`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_meta_instance_app_relation` (
  `instance` varchar(128) NOT NULL,
  `app` varchar(128) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_meta_privilege`
--

DROP TABLE IF EXISTS `dpaa_meta_privilege`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_meta_privilege` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(128) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(128) NOT NULL,
  `updated_at` datetime NOT NULL,
  `table_id` int(11) NOT NULL COMMENT 'TABLE',
  `grantee` varchar(128) DEFAULT NULL COMMENT 'grantee',
  `privilege` varchar(128) DEFAULT NULL COMMENT 'privilege',
  `md5` varchar(128) DEFAULT NULL COMMENT 'md5',
  `check_time` datetime NOT NULL COMMENT '自动收集时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_dmpri_tabid_gra_pri` (`table_id`,`grantee`,`privilege`)
) ENGINE=InnoDB AUTO_INCREMENT=34971 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_meta_room`
--

DROP TABLE IF EXISTS `dpaa_meta_room`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_meta_room` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(128) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(128) NOT NULL,
  `updated_at` datetime NOT NULL,
  `room_name` varchar(128) NOT NULL,
  `sort` int(11) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_meta_schema`
--

DROP TABLE IF EXISTS `dpaa_meta_schema`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_meta_schema` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(128) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(128) NOT NULL,
  `updated_at` datetime NOT NULL,
  `db_type` varchar(128) NOT NULL COMMENT 'ORACLE | MYSQL',
  `instance_name` varchar(128) NOT NULL COMMENT 'INSTANCE',
  `schema_name` varchar(128) NOT NULL COMMENT 'SCHEMA名称',
  `default_tablespace` varchar(128) DEFAULT NULL,
  `create_time` datetime DEFAULT NULL,
  `used` int(11) NOT NULL,
  `check_time` datetime NOT NULL COMMENT '自动收集时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_dms_name_insid` (`instance_name`,`schema_name`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_meta_server`
--

DROP TABLE IF EXISTS `dpaa_meta_server`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_meta_server` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(128) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(128) NOT NULL,
  `updated_at` datetime NOT NULL,
  `hostname` varchar(128) NOT NULL COMMENT 'hostname',
  `room` varchar(512) DEFAULT NULL COMMENT '机房',
  `cpu_module_name` varchar(512) DEFAULT NULL COMMENT 'CPU类型',
  `cpu_mhz` varchar(512) DEFAULT NULL COMMENT 'CPU频率',
  `memory_gb` varchar(512) DEFAULT NULL COMMENT '内存大小',
  `primary_disk_type` varchar(512) DEFAULT NULL COMMENT '存储类型',
  `disk_space_gb` varchar(512) DEFAULT NULL COMMENT '存储容量',
  `comment` varchar(4000) DEFAULT NULL COMMENT '备注',
  `host_used` int(11) DEFAULT NULL COMMENT 'HOST是否可用 0-不可用 1-可用',
  `check_time` datetime NOT NULL COMMENT '自动收集时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_meta_server_hostname` (`hostname`)
) ENGINE=InnoDB AUTO_INCREMENT=126 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_meta_server_ip_relation`
--

DROP TABLE IF EXISTS `dpaa_meta_server_ip_relation`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_meta_server_ip_relation` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime(6) NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime(6) NOT NULL,
  `server_id` int(11) DEFAULT NULL,
  `ip_address` varchar(128) DEFAULT NULL COMMENT 'IP',
  `ip_type` varchar(128) DEFAULT NULL COMMENT 'IP作用, APP OR DATA or VIP',
  `dnsname` varchar(1024) DEFAULT NULL COMMENT 'DNSNAME 多个',
  `ip_used` int(11) DEFAULT NULL COMMENT '是否可用 0-不可用 1-可用',
  `comment` varchar(4000) DEFAULT NULL,
  `check_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_meta_phy_ip_rel` (`ip_address`)
) ENGINE=InnoDB AUTO_INCREMENT=258 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_meta_table`
--

DROP TABLE IF EXISTS `dpaa_meta_table`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_meta_table` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(128) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(128) NOT NULL,
  `updated_at` datetime NOT NULL,
  `name` varchar(128) NOT NULL COMMENT 'TABLE名称',
  `schema_id` int(11) NOT NULL COMMENT 'SCHEMA',
  `tab_comment` varchar(1024) DEFAULT NULL COMMENT 'table comments',
  `sequence_name` varchar(128) DEFAULT NULL COMMENT 'table 对应的SEQUENCE',
  `is_partition` int(11) DEFAULT NULL COMMENT 'table是否分区表',
  `partition_methord` varchar(128) DEFAULT NULL COMMENT '分区逻辑方式',
  `partition_col` varchar(512) DEFAULT NULL COMMENT 'table是否分区表',
  `partition_interval` varchar(512) DEFAULT NULL COMMENT '分区策略',
  `table_public_synonym` varchar(128) DEFAULT NULL COMMENT '表公共同义词',
  `seq_public_synonym` varchar(128) DEFAULT NULL COMMENT '序列公共同义词',
  `seq_opr_grant` varchar(128) DEFAULT NULL COMMENT 'sequence对opr用户授权SELECT',
  `check_time` datetime NOT NULL COMMENT '自动收集时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_dmt_name_schid` (`name`,`schema_id`)
) ENGINE=InnoDB AUTO_INCREMENT=16994 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_meta_table_custom`
--

DROP TABLE IF EXISTS `dpaa_meta_table_custom`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_meta_table_custom` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(128) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(128) NOT NULL,
  `updated_at` datetime NOT NULL,
  `instance_name` varchar(128) DEFAULT NULL,
  `schema_name` varchar(128) DEFAULT NULL,
  `table_name` varchar(128) DEFAULT NULL,
  `sequence_name` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_out_ip_manage`
--

DROP TABLE IF EXISTS `dpaa_out_ip_manage`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_out_ip_manage` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime(6) NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime(6) NOT NULL,
  `server_id` int(11) NOT NULL,
  `vendor` varchar(128) DEFAULT NULL,
  `out_ip` varchar(128) DEFAULT NULL,
  `command_path` varchar(512) DEFAULT NULL,
  `comment` varchar(4000) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_dpaa_out_ip_manage_ip` (`server_id`)
) ENGINE=InnoDB AUTO_INCREMENT=28 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_service_request_log`
--

DROP TABLE IF EXISTS `dpaa_service_request_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_service_request_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime(6) NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime(6) NOT NULL,
  `ip` varchar(4000) DEFAULT NULL,
  `username` varchar(100) DEFAULT NULL,
  `secret_key` varchar(100) DEFAULT NULL,
  `request_type` varchar(100) DEFAULT NULL,
  `request_method` varchar(100) DEFAULT NULL,
  `handle_type` varchar(100) DEFAULT NULL,
  `request_data` longtext,
  `response_data` longtext,
  `async_response_data` longtext,
  `comments` longtext,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=276 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dpaa_system_variables_conf`
--

DROP TABLE IF EXISTS `dpaa_system_variables_conf`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpaa_system_variables_conf` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) DEFAULT NULL,
  `created_at` datetime(6) NOT NULL,
  `updated_by` varchar(30) DEFAULT NULL,
  `updated_at` datetime(6) NOT NULL,
  `db_type` varchar(64) NOT NULL,
  `parameter_name` varchar(256) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dss_meta_instance`
--

DROP TABLE IF EXISTS `dss_meta_instance`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dss_meta_instance` (
  `id` int(20) NOT NULL AUTO_INCREMENT,
  `ip` varchar(33) DEFAULT NULL,
  `fio_type` varchar(10) DEFAULT NULL,
  `db_type` varchar(55) DEFAULT NULL,
  `sid` varchar(22) DEFAULT NULL,
  `sid_roomrole` varchar(22) DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `is_expdp` varchar(10) DEFAULT NULL,
  `awrid` int(11) DEFAULT NULL,
  `redisname` varchar(255) DEFAULT NULL,
  `db_ver` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_dss_meta_instance` (`ip`,`sid`),
  KEY `ind1_dss_meta_instance` (`updated_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `erase_o_table_conf`
--

DROP TABLE IF EXISTS `erase_o_table_conf`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `erase_o_table_conf` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime NOT NULL,
  `table_name` varchar(128) DEFAULT NULL,
  `schema_name` varchar(128) DEFAULT NULL,
  `erase_o_schema_name` varchar(128) DEFAULT NULL,
  `is_exist_source` tinyint(4) DEFAULT '0',
  `problem_proj` varchar(512) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_table_name` (`table_name`,`schema_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `http_request_log`
--

DROP TABLE IF EXISTS `http_request_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `http_request_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime(6) NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime(6) NOT NULL,
  `ip` varchar(4000) DEFAULT NULL,
  `username` varchar(100) DEFAULT NULL,
  `secret_key` varchar(100) DEFAULT NULL,
  `direction` varchar(100) DEFAULT NULL,
  `request_type` varchar(100) DEFAULT NULL,
  `request_data` longtext,
  `comments` longtext,
  `response_data` longtext,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `mha_dns_list`
--

DROP TABLE IF EXISTS `mha_dns_list`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `mha_dns_list` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime(6) NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime(6) NOT NULL,
  `current_ip` varchar(128) DEFAULT NULL,
  `dns_name` varchar(128) DEFAULT NULL,
  `port` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_mha_dns_list_dns_name` (`dns_name`),
  UNIQUE KEY `uk_mha_dns_list_ip_port` (`current_ip`,`port`),
  KEY `uk_mha_dns_list_ip` (`current_ip`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `shm_test_1`
--

DROP TABLE IF EXISTS `shm_test_1`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `shm_test_1` (
  `a` int(11) DEFAULT NULL,
  `b` tinyint(4) DEFAULT NULL,
  `c` smallint(6) DEFAULT NULL,
  `d` mediumint(9) DEFAULT NULL,
  `e` bigint(20) DEFAULT NULL,
  `f` float DEFAULT NULL,
  `g` double DEFAULT NULL,
  `h` decimal(10,0) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `shm_test_2`
--

DROP TABLE IF EXISTS `shm_test_2`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `shm_test_2` (
  `a` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `sql_exec`
--

DROP TABLE IF EXISTS `sql_exec`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sql_exec` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime(6) NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime(6) NOT NULL,
  `sql_unique` varchar(128) NOT NULL,
  `sql_text` longtext NOT NULL,
  `db_type` varchar(128) NOT NULL,
  `instance_name` varchar(128) DEFAULT NULL,
  `schema_name` varchar(128) NOT NULL,
  `bettle_id` bigint(20) DEFAULT NULL,
  `file_id` bigint(20) DEFAULT NULL,
  `sql_piece_id` bigint(20) DEFAULT NULL,
  `sql_type` varchar(128) DEFAULT NULL,
  `sql_usage` varchar(128) DEFAULT NULL,
  `flow_status` varchar(128) DEFAULT NULL,
  `sql_status` varchar(128) DEFAULT NULL,
  `client_send_time` datetime(3) DEFAULT NULL,
  `server_recv_time` datetime(3) DEFAULT NULL,
  `server_send_time` datetime(3) DEFAULT NULL,
  `client_recv_time` datetime(3) DEFAULT NULL,
  `connect_url` varchar(128) DEFAULT NULL,
  `message` longtext NOT NULL,
  `session_elapsed_time` int(11) DEFAULT NULL,
  `sql_elapsed_time` int(11) DEFAULT NULL,
  `effect_row` int(11) DEFAULT NULL,
  `exec_result` longtext,
  PRIMARY KEY (`id`),
  UNIQUE KEY `sql_unique` (`sql_unique`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `sr_check_tag_task`
--

DROP TABLE IF EXISTS `sr_check_tag_task`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sr_check_tag_task` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_by` varchar(30) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_by` varchar(30) NOT NULL,
  `updated_at` datetime NOT NULL,
  `app_name` varchar(250) DEFAULT NULL,
  `tag` varchar(20) DEFAULT NULL,
  `all_sqlmap_md5sum` varchar(100) DEFAULT NULL,
  `status` varchar(250) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `app_name` (`app_name`,`tag`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2019-12-03 18:33:39
