# 文档记录

## 一、目录介绍
```
./examples                          # 阿里云官方提供测试样例
./manual_compile_jar                # 手动编译后的jar,去除掉沙箱限制的类调用
./sqlscripts/create_table           # 创建表结构SQL
./sqlscripts/extract_features       # 提取特征SQL
./sqlscripts/json_data_flat         # JSON数据扁平化SQL
./sqlscripts/scripts                # 
./sqlscripts/sync_sql               # 同步数据SQL
./src                               # 程序源码
./target                            # 程序编译结果
./warehouse                         # 阿里云官方提供测试样例
```

## 二、在线文档资源

[MaxCompute在线文档](https://helpcdn.aliyun.com/product/27797.html?spm=5176.doc27800.3.1.qTOZfV)

[MaxCompute数据上传与下载](https://helpcdn.aliyun.com/document_detail/51656.html?spm=5176.doc27827.6.586.Wq1AGj)

[MaxCompute的SQL语法](https://helpcdn.aliyun.com/document_detail/27860.html?spm=5176.doc27860.3.3.uVRZmD)

[MaxCompute的JAVA UDF开发文档](https://helpcdn.aliyun.com/document_detail/27867.html?spm=5176.7854976.6.623.MjpJsx)

[MaxComputeStudio在线文档](https://helpcdn.aliyun.com/document_detail/50891.html?spm=5176.doc50891.3.3.KHRmB1)

[MaxCompute客户端命令行工具](https://helpcdn.aliyun.com/document_detail/27971.html?spm=5176.doc50891.6.725.bYtKAt)

[MaxCompute 常用命令简介](https://helpcdn.aliyun.com/document_detail/27827.html?spm=5176.doc27827.3.3.4SykF5)

[MaxCompute大数据开发套件在线文档](https://help.aliyun.com/product/30254.html?spm=5176.7847677.3.1.3bnDfw)

[MaxCompute MapReduce及UDF程序在分布式环境中运行时受到Java沙箱的限制](httphttps://help.aliyun.com/document_detail/27967.html)


## 三、UDF的开发与运行
参考地址: [https://help.aliyun.com/document_detail/50902.html?spm=5176.doc50898.6.743.aeu8xc](https://help.aliyun.com/document_detail/50902.html?spm=5176.doc50898.6.743.aeu8xc)

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

## 四、UDTF与UDAF开发与运行
1. 与UDF类似。


## 五、将编写好的UDF打包，上传资源库，注册成函数
```
gradle install  com.jayway.JsonPath lib

mvn clean package mc_luna.jar

add jar d:/mc_luna.jar -f

CREATE FUNCTION test_udtf AS 'myudf.UDTFHello' USING 'mc_luna.jar';


CREATE FUNCTION test_udf AS 'myudf.UDFPlusHello' USING 'mc_luna.jar';

// 测试udtf

select mytest_udtf(bank_name,bank_name) from odps_ebank_detail limit 1;

// 测试udf 

select test_udf(person_id,account_info) from odps_ebank_detail limit 1;
```


## 六、开发经验总结
1. 因为Maxcompute开发的udf或mr时在maxcompute上运行存在沙箱限制，不允许特定包进行调用。
所以在开发过程中，如果使用了第三方Jar包来完成部分功能时，需要手动将调用限制那块的代码去除掉，然后重新打成jar包。
目前我们使用`/manual_compile_jar`目录为存放重新编译的jar包，如果你使用maven打包程序需要该包替换到maven目录下自动下载的包。