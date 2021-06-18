
<div align=center><img src="./images/openmldb_logo.png"/></div>

- [**Slack Channel**](https://hybridsql-ws.slack.com/archives/C01R7L7AL3W)
- [**Discussions**](https://github.com/4paradigm/OpenMLDB/discussions)
- [**README中文**](./README_cn.md)

## Introduction

OpenMLDB is the RMCP(Real-time/Massive-parallel Consistent Processing) database for ML development.

Developmenting a ML application includes the offline big-data processing and online feature execution. RMCP databse supports online process and massive-parallel processing at the same time. It ensures the consistency of data and computing to meet the requirements of ML development. OpenMLDB is the high-performance RMCP database which is based on LLVM optimization and supports ANSI SQL for AI. The performance of batch process is 6x times better than mainstream MPP framework. The performance of real-time process is 8x times better thant the mainstream memory databases. OpenMLDB relis on the unified computing and storage engines to ensure the consistency which highly reduce the cost of AI landing and help more developers to deploy machine applications at scale.

![Architecture](./images/openmldb_architecture.png)

## Features

* **High Performance**
    OpenMLDB implement the native SQL compiler with C++ and LLVM. It contains tens of optimization passes for physical plans and expressions. It can generate optmized binary code for different hardware and optmize the memory layout for feature storage. The storage and cost for features can be 9x times lower than the similar databases. The performance of real-time execution can be 9x times better and the performance of batch processing can be 6x times better.

* **Consistency**

    OpenMLDB ensures the consistency for online and offline. Data scientists can use OpenMLDB for feature extration which avoids 
    OpenMLDB首先保证在线和离线特征计算一致性，科学家使用OpenMLDB建模生成的特征，可规避特征穿越等问题，上线后使用相同LLVM IR进行编译优化，保证与在线特征计算逻辑一致。其次保证数据存储一致性，数据从离线到在线进行实时同步，用户不需要为离线和在线管理不同数据源，也避免数据不一致对特征和模型带来的影响。

* **High Availability**

    OpenMLDB的大规模并行计算服务和数据库存储服务，都支持多节点都分布式高可用特性，可以自动Failover和动态扩缩容，避免单点故障。

* **SQL Support**

    OpenMLDB支持用户友好SQL接口，兼容大部分ANSI SQL语法以及针对AI场景拓展了新的SQL特性。以时序特征抽取为例，支持标准SQL的Over Window语法，还针对AI场景需求进行拓展，支持基于样本表滑窗的Window Union语法，实时计算引擎支持基于当前行的Request Mode窗口聚合计算。

* **AI Optimization**

    OpenMLDB以面向ML应用开发优化为目标，架构设计以及实现上都针对AI进行大量优化。在存储方面以高效的数据结构存储特征数据，无论是内存利用率还是实时查询效率都比同类型产品高数倍，而计算方面提供了机器学习场景常用的特殊拼表操作以及特征抽取相关UDF/UDAF支持，基本满足生产环境下机器学习特征抽取和上线的应用需求。

* **Easy To Use**

    OpenMLDB使用门槛与普通数据库接近，无论是建模科学家还是应用开发者都可以使用熟悉的SQL进行开发，并且同时支持ML应用落地所必须的离线大数据批处理服务以及在线特征计算服务，使用一个数据库产品就可以低成本实现AI落地闭环。

## Performance

与主流在线数据库产品相比，在不同的数据规模以及计算复杂度下，OpenMLDB的实时请求性能都比其他方案有数倍甚至数十倍的提升。

![Online Benchmark](./images/online_benchmark.png)

在大数据批处理模式下，使用OpenMLDB进行特征抽取，相比业界最流行的Spark社区版，离线性能在窗口数据倾斜优化下有数倍提升，大大降低离线计算的TCO。

![Offline Benchmark](./images/offline_benchmark.png)

## QuickStart

使用OpenMLDB快速开发和上线ML应用，以Kaggle比赛Predict Taxi Tour Duration项目为例。

```bash
# 启动docker镜像
docker run -it 4pdosc/openmldb:1.0.0 bash
 
# 初始化环境
sh init.sh
 
# 导入行程历史数据到OpenMLDB
python3 import.py
 
# 使用行程数据进行模型训练
sh train.sh
 
# 使用训练的模型搭建链接OpenMLDB的实时推理HTTP服务
sh start_predict_server.sh
 
# 通过http请求发送一个推理请求
python3 predict.py
```

## Status and Roadmap

### Status of Project

* SQL编译器和优化器[完成]
    * 支持基础ANSI SQL语法解析[完成]
    * 支持物理计划和表达式优化[完成]
    * 支持计算函数代码生成[完成]
* 前端编程接口[开发中]
    * 支持标准JDBC协议[完成]
    * 支持C++、Python SDK[完成]
    * 支持RESTful API[开发中]
* 在线离线计算引擎[完成]
    * 在线数据库计算引擎[完成]
    * 离线批处理计算引擎[完成]
* 统一存储引擎[开发中]
    * 分布式高性能内存存储[完成]
    * 在线离线数据一致性同步[开发中]

### Roadmap

* SQL兼容
    * 完善ANSI SQL支持，执行引擎支持GroupBy等语法[2021H2]
    * 针对AI场景扩展特有的语法特性和UDAF函数[2021H2]
* 性能优化
    * 面向批式数据处理和在线数据处理场景的逻辑和物理计划优化[2021H2]
    * 支持高性能分布式执行计划生成和代码生成[2021H2]
    * 更多经典SQL表达式优化过程支持[2022H1]
    * 离线计算引擎集成针对机器学习场景的Native LastJoin优化过程[2021H2]
    * 存储引擎使用面向时序优化的内存分配和回收策略，能大幅减少内存碎片[2022H1]
* 生态集成
    * 适配多种行编码格式和列编码格式，兼容Apache Arrow格式和生态[2021H2]
    * 适配流式等主流开源SQL计算框架，如优化FlinkSQL执行引擎等[2022H1]
    * 支持主流编程语言接口，包括C++, Java, Python, Go, Rust SDK等[2021H2]
    * 支持PMEM等新型存储硬件[2022H1]
    * 存储引擎兼容Flink、Kafka、Spark connector[2022H1]

## License

[Apache License 2.0](./LICENSE)
