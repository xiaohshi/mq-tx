## 基于RabbiMQ的分布式事务Demo

一个简单的基于rabbitMQ的分布式事务demo程序，在极端情况下存在消息丢失的风险，没有保证强一致性

### 原理

基于rabbitmq的分布式事务的主要解决以下两个问题：

1、如何保证消息100%被送到rabbitMq

利用rabbitMq的确认机制，当生产者发送消息后，会回传一个ack确认，没有收到该消息，则进行消息重传；

2、如何保证消费者100%的接收到消息并消费

利用手动消息确认，可以保证事务一定会被接受消息并消费，对于没有消费成功的事务，可以选择重传或者其它逻辑处理；消费端利用redis进行幂等处理，保证同一条消息只被处理一次

<img src="./mq.png" alt="mq" style="zoom: 67%;" />

### 异常分析

讨论各个阶段出错出现的问题，以及给出的处理方案，不一定合理，所有的组件都需要进行高可用保证

#### 生产端

| 异常                               | 结果                         | 处理方案                   |
| ---------------------------------- | ---------------------------- | -------------------------- |
| 在本地事务执行之前报错             | 不影响一致性                 | 直接返回                   |
| 在本地事务执行中报错               | 不影响一致性       | 执行失败策略               |
| 本地事务执行完成，发送消息之前报错 | 影响一致性 | 未解决                     |
| 消息没有投递到rabbitMq             | 影响一致性                   | 重试策略                   |
| 消息投递成功，但是没有收到确认     | 不影响一致性                 | 等待确认，超时重传消息         |
| 发送消息失败                       | 影响一致性 | 事务回滚（反操作）或其它逻辑处理 |

#### 消费端

| 异常                       | 结果         | 处理方案                         |
| -------------------------- | ------------ | -------------------------------- |
| 在本地事务执行之前报错     | 影响一致性   | 未解决，这个一般情况下不会出错              |
| 在本地事务执行中报错       | 不影响一致性 | 不断重试，直至成功               |
| 本地事务执行完成，确认出错 | 不影响一致性 | 重新拉取消息，有幂等处理保证不会重复消费 |

### 后期计划

尽量保证做成强一致性

### 有待讨论的问题

以下几个问题需要验证

1、在消费者事务和确认分开执行，事务失败会不断重试，这种模式是不是比事务和确认一起执行更好？

2、在生产者发送消息的时候消息发送失败该如何处理，事务需不需要回滚操作？

3、在生产者是否需要加入本地消息表，用来持久化消息，后台线程遍历该表将消息投递到rabbitMq中？

### Used
gorm

go-redis

rabbitMq