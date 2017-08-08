gradle install  com.jayway.JsonPath lib

mvn clean package mc_luna.jar

add jar d:/mc_luna.jar -f

CREATE FUNCTION test_udtf AS 'myudf.UDTFHello' USING 'mc_luna.jar';


CREATE FUNCTION test_udf AS 'myudf.UDFPlusHello' USING 'mc_luna.jar';

// 测试udtf

select mytest_udtf(bank_name,bank_name) from odps_ebank_detail limit 1;

// 测试udf 

select test_udf(person_id,account_info) from odps_ebank_detail limit 1;
