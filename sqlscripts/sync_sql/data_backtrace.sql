# 快照表
CREATE TABLE odps_backtrace_test (
	id BIGINT,
	name STRING,
	value STRING
)PARTITIONED BY (
	pt STRING
)
LIFECYCLE 100000

# 增量表
CREATE TABLE odps_backtrace_odps_backtrace_test_inc_list LIKE odps_backtrace_test;

# 快照记录数据
INSERT INTO TABLE odps_backtrace_test PARTITION (pt='20170727')
SELECT 1, 'A', '001' FROM odps_statistics_verification LIMIT 1
UNION ALL
SELECT 2, 'B', '002' FROM odps_statistics_verification LIMIT 1
UNION ALL
SELECT 3, 'C', '003' FROM odps_statistics_verification LIMIT 1
UNION ALL
SELECT 4, 'D', '004' FROM odps_statistics_verification LIMIT 1
UNION ALL
SELECT 5, 'E', '005' FROM odps_backtrace_test_inc_list LIMIT 1
UNION ALL
SELECT 6, 'F', '006' FROM odps_statistics_verification LIMIT 1
UNION ALL
SELECT 7, 'G', '007' FROM odps_statistics_verification LIMIT 1


# 增量数据1
INSERT INTO TABLE odps_backtrace_test_inc_list PARTITION (pt='20170728')
SELECT 1, 'A', '100' FROM odps_statistics_verification LIMIT 1
UNION ALL
SELECT 2, 'B', '200' FROM odps_statistics_verification LIMIT 1

# 增量数据2
INSERT INTO TABLE odps_backtrace_test_inc_list PARTITION (pt='20170729')
SELECT 1, 'A', '101' FROM odps_statistics_verification LIMIT 1
UNION ALL
SELECT 3, 'C', '300' FROM odps_statistics_verification LIMIT 1


# 数据回溯SQL
SELECT  
	CASE WHEN b.id IS NOT NULL THEN b.id ELSE a.id END AS id,
	CASE WHEN b.name IS NOT NULL THEN b.name ELSE a.name END AS name,
	CASE WHEN b.value IS NOT NULL THEN b.value ELSE a.value END AS value,
	CASE WHEN b.pt IS NOT NULL THEN b.pt ELSE a.pt END AS pt
FROM
odps_backtrace_test a 
FULL OUTER JOIN 
(
  SELECT t.id ,t.name, t.value, t.pt
  FROM (
	SELECT id, name, value, pt, ROW_NUMBER() OVER (PARTITION BY id ORDER BY pt DESC) AS new
	FROM odps_backtrace_test_inc_list --如需恢复到指定时间指增加条件：WHERE pt <= 20170728
  ) t
  WHERE t.new = 1
) b
ON a.id  = b.id
ORDER BY id ASC
LIMIT 1000;