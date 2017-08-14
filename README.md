参见 https://help.aliyun.com/document_detail/27984.html?spm=5176.doc27811.2.5.L2ouQI
## UDF的开发与运行
1. 从git拉取项目至本地。如E:\mc_luna。
2. 使用安装好MaxCompute插件的Intellij IDEA打开，确保已配置好可用的MaxCompute项目，例如RMDC_DW。
3. 定义继承UDF父类的子类，实现函数。例如：src/main/java/myudf/GetCountByDate
4. 在项目中右键或者使用快捷键*ctrl+alt+F10*运行。初次运行会弹出配置对话框。需要依次填入：
	- MaxCompute Project
	- maxcompute table
	- table partition 
	- table columns
	
	其中table columns的个数与类型应该符合定义的函数参数列表。
5. 配置完毕后运行即可。

## UDTF与UDAF开发与运行
1. 与UDF类似。

## 将编写好的UDF注册
gradle install  com.jayway.JsonPath lib

mvn clean package mc_luna.jar

add jar d:/mc_luna.jar -f

CREATE FUNCTION test_udtf AS 'myudf.UDTFHello' USING 'mc_luna.jar';


CREATE FUNCTION test_udf AS 'myudf.UDFPlusHello' USING 'mc_luna.jar';

// 测试udtf

select mytest_udtf(bank_name,bank_name) from odps_ebank_detail limit 1;

// 测试udf 

select test_udf(person_id,account_info) from odps_ebank_detail limit 1;
