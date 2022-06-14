package com.cmbc.kafka.transaction.monitoring;/*
 * @Package com.cmbc.kafka.transaction.monitoring
 * @author wang shuangli
 * @date 2022-05-13 13:36
 * @version V1.0
 * @Copyright © 2015-2021
 */

import com.alibaba.fastjson.JSONObject;
import com.cmbc.util.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;


public class noTranTypeSYSSA {
    private static final Logger logger = LoggerFactory.getLogger(noTranTypeSYSSA.class);

    public static void main(String[] args) throws Exception {
        //校验参数
        if (args.length != 2) {
            System.out.println("程序输入参数异常，应输入两个参数：1.配置文件在hdfs上的地址 2.配置文件所属用户名，请检查！");
            System.exit(0);
        }
        String configPath = args[0];
        String user = args[1];
        //获取StreamExecutionEnvironment运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        PropertiesUtil.initProperties(configPath, user);
        //flink作业名称
        String appName = PropertiesUtil.getString("app_name");
        //周期性生成检查点间隔
        int checkpointInterval = PropertiesUtil.getInt("checkpoint_interval");
        //检查点文件保存的目录，应为hdfs目录
        String checkpointDataUri = PropertiesUtil.getString("checkpointdatauri");
        //当前作业并行度设置，此设置已废弃，可在提交作业时，指定并行度
        int parallelism = PropertiesUtil.getInt("parallelism");
        //输入kafka的集群地址信息
        String sourceKafkaBootstrapServers = PropertiesUtil.getString("source_kafka_bootstrap_servers");
        //kafka的消费者组ID
        String sourceKafkaGroupId = PropertiesUtil.getString("source_kafka_group_id");
        //消费的kafka的topic名称
        String sourceKafkaTopic = PropertiesUtil.getString("source_kafka_topic");
        //指定消费kafka初始偏移量
        String sourceKafkaOffset = PropertiesUtil.getString("source_kafka_offset");
        //给人行报送的系统(指民生银行)编码
        String institution = PropertiesUtil.getString("institution");
        //需要给人行报送的系统清单，逗号分隔
        String[] system = PropertiesUtil.getString("system").split(",");
        //需要给人行报送的系统编码清单，逗号分隔
        String[] systemCode = PropertiesUtil.getString("system_code").replace(" ", "").split(",");
        //需要给人行报送的系统主机地址清单，逗号分隔
        String[] systemHost = PropertiesUtil.getString("system_host").replace(" ", "").split(",");
        //需要给人行报送的系统应用编码清单，逗号分隔
        String[] systemSpv = PropertiesUtil.getString("system_spv").replace(" ", "").split(",");
        //需要给人行报送的系统应用的模块编码清单，逗号分隔
        String[] systemIntf = PropertiesUtil.getString("system_intf").replace(" ", "").split(",");
        //推送到的kafka集群地址
        String sinkKafkaBootstrapServers = PropertiesUtil.getString("sink_kafka_bootstrap_servers");
        //推送到的kafka的topic清单，注意：每个系统推送的topic不同
        String[] sinkKafkaTopic = PropertiesUtil.getString("sink_kafka_topic").replace(" ", "").split(",");
        //判断系统健康度(即healthy_biz)的条件，条件为kafka消息中原始原始字段加上>、<、= 多条件时用&& ||连接，eg：resp_p>0.9&&succ_p>0.9
        String triggerCondition = PropertiesUtil.getString("trigger_condition").replace(" ", "");
        //是否需要推送至clickhouse，为true或者flase
        boolean isSinkClickhouse = PropertiesUtil.getBoolean("is_sink_clickhouse");
        //推送的clickhouse的地址
        String sinkClickhouseHost = PropertiesUtil.getString("sink_clickhouse_host");
        //推送的clickhouse的端口
        int sinkClickhousePort = PropertiesUtil.getInt("sink_clickhouse_port");
        //推送的clickhouse的用户名
        String sinkClickhouseUsername = PropertiesUtil.getString("sink_clickhouse_username");
        //推送的clickhouse的密码
        String sinkClickhousePassword = PropertiesUtil.getString("sink_clickhouse_password");
        //推送的clickhouse的数据库，需要提前创建好
        String sinkClickhouseDatabase = PropertiesUtil.getString("sink_clickhouse_database");
        //推送的clickhouse的表，需要提前创建好
        String sinkClickhouseTable = PropertiesUtil.getString("sink_clickhouse_table");


        //初始化配置文件，加载配置信息
        //initProperties(configPath, user);

        //生成配置信息，用来过滤kafka源中指定系统的message
        Map<String, String[]> sysCodeConfig = sysCodeConfigBuilder(system, systemCode, systemHost, systemSpv, systemIntf, sinkKafkaTopic);
        //3.设置流处理的时间为 EventTime ，使用数据发生的时间来进行数据处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //将Flink默认的开发环境并行度设置为1
        //env.setParallelism(parallelism);
        //保证程序长时间运行的安全性进行checkpoint操作,checkpointInterval秒启动一次checkpoint
        env.enableCheckpointing(checkpointInterval);
        // 设置checkpoint只checkpoint一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置两次checkpoint的最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        // checkpoint超时的时长
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 允许的最大checkpoint并行度
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //当程序关闭的时，触发额外的checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置checkpoint的地址
        env.setStateBackend(new FsStateBackend(checkpointDataUri));

        //consumer properties
        Properties consumerProperties = getConsumerProperties(sourceKafkaBootstrapServers, sourceKafkaGroupId, sourceKafkaOffset);
        //producer properties


        //获取kafkaSource
/*        KafkaSource<String> kafkaSource = getKafkaSource(sourceKafkaBootstrapServers, sourceKafkaTopic, sourceKafkaGroupId, sourceKafkaOffset);
        DataStreamSource<String> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");*/
        //添加数据源
        DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer<String>(
                sourceKafkaTopic,
                new SimpleStringSchema(),
                consumerProperties
        ).setCommitOffsetsOnCheckpoints(true));

        /*KafkaSource<String> kafkaSource = getKafkaSource(sourceKafkaBootstrapServers, sourceKafkaTopic, sourceKafkaGroupId, sourceKafkaOffset);
        DataStreamSource<String> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "noTranTypeTopicData");*/

//        stream.print("Kafka01");
        OutputTag<String> t_fac_counter = new OutputTag<String>("t_fac_counter", Types.STRING);
        OutputTag<String> t_fac_nbank_per = new OutputTag<String>("t_fac_nbank_per", Types.STRING);
        OutputTag<String> t_fac_mainindex = new OutputTag<String>("t_fac_mainindex", Types.STRING);
        String t_fac_counter_topic = sinkKafkaTopic[0];
        String t_fac_nbank_per_topic = sinkKafkaTopic[1];
        String t_fac_mainindex_topic = sinkKafkaTopic[2];

        SingleOutputStreamOperator<String> fullMsgStream = stream.map(msg -> messageEncapsulation(msg, sysCodeConfig, institution, triggerCondition))
                .filter(msg -> !msg.isEmpty())
                .process(new ProcessFunction<JSONObject, String>() { //将不同系统的数据分流到不同的侧输出流中
                    @Override
                    public void processElement(JSONObject s, Context ctx, Collector<String> out) throws Exception {
                        if (systemCode[0].equals(s.getString("system_code"))) { //t_fac_counter topic的侧输出流
                            ctx.output(t_fac_counter, s.toJSONString());
                        } else if (systemCode[1].equals(s.getString("system_code"))) {//t_fac_nbank_per topic的侧输出流
                            ctx.output(t_fac_nbank_per, s.toJSONString());
                        } else {
                            out.collect(s.toJSONString());//t_fac_mainindex topic的侧输出流
                        }
                    }
                });
        //sink：kafka
        //输出到topic：t_fac_counter
        fullMsgStream.getSideOutput(t_fac_counter)
                .addSink(new FlinkKafkaProducer<String>(
                                // 目标 topic
                                "",
                                // 序列化 schema
                                new CustomKafkaSerializationSchema(t_fac_counter_topic),
//                        new MySerializationSchema(),
                                getProducerProperties(sinkKafkaBootstrapServers),
                                // 容错
                                FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
                )
                .name(t_fac_counter_topic);
        //输出到topic：t_fac_nbank_per_topic
        fullMsgStream.getSideOutput(t_fac_nbank_per)
                .addSink(new FlinkKafkaProducer<String>(
                                // 目标 topic
                                "",
                                // 序列化 schema
                                new CustomKafkaSerializationSchema(t_fac_nbank_per_topic),
//                        new MySerializationSchema(),
                                getProducerProperties(sinkKafkaBootstrapServers),
                                // 容错
                                FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
                ).name(t_fac_mainindex_topic);
        //输出到topic：t_fac_counter
        fullMsgStream
                .addSink(new FlinkKafkaProducer<String>(
                                // 目标 topic
                                "",
                                // 序列化 schema
                                new CustomKafkaSerializationSchema(t_fac_mainindex_topic),
//                        new MySerializationSchema(),
                                getProducerProperties(sinkKafkaBootstrapServers),
                                // 容错
                                FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
                )
                .name(t_fac_mainindex_topic);

       /* //输出到topic：t_fac_counter
        fullMsgStream.getSideOutput(t_fac_counter)
                .sinkTo(KafkaSink.<String>builder()
                        .setKafkaProducerConfig(getProducerProperties(sinkKafkaBootstrapServers))
                        .setBootstrapServers(sinkKafkaBootstrapServers)
                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic(t_fac_counter_topic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                        )
                        .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .build())
                .name(t_fac_counter_topic);
        //输出到topic：t_fac_nbank_per_topic
        fullMsgStream.getSideOutput(t_fac_nbank_per)
                .sinkTo(KafkaSink.<String>builder()
                        .setKafkaProducerConfig(getProducerProperties(sinkKafkaBootstrapServers))
                        .setBootstrapServers(sinkKafkaBootstrapServers)
                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic(t_fac_nbank_per_topic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                        )
                        .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .build())
                .name(t_fac_nbank_per_topic);
        //输出到topic：t_fac_counter
        fullMsgStream
                .sinkTo(KafkaSink.<String>builder()
                        .setKafkaProducerConfig(getProducerProperties(sinkKafkaBootstrapServers))
                        .setBootstrapServers(sinkKafkaBootstrapServers)
                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic(t_fac_mainindex_topic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                        )
                        .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .build())
                .name(t_fac_mainindex_topic);*/


        //sink:clickhouse
        /*if (isSinkClickhouse) {
            SingleOutputStreamOperator<FullMsg> ckStream = fullMsgStream.map(jsonString -> {
                        JSONObject jsonObject = JSONObject.parseObject(jsonString);
                        JSONObject status = JSONObject.parseObject(jsonObject.getString("status"));
                        return FullMsg.of(jsonObject.getString("time"),
                                jsonObject.getString("institution"),
                                jsonObject.getString("system_code"),
                                jsonObject.getString("healthy_biz"),
                                jsonObject.getString("healthy_sys"),
                                status.getIntValue("RV"),
                                status.getDoubleValue("RS"),
                                status.getIntValue("RD"),
                                status.getDoubleValue("RR"),
                                jsonObject.getString("faults")
                        );
                    }
            );

            //SINK Clickhouse
            String sql = "INSERT INTO default." + sinkClickhouseTable + " (time, institution, system_code,healthy_biz,healthy_sys,RV,RS,RD,RR,faults) VALUES (?,?,?,?,?,?,?,?,?,?)";
            SinkClickHouseUtil jdbcSink = new SinkClickHouseUtil(sql, sinkClickhouseHost, sinkClickhousePort, sinkClickhouseDatabase, sinkClickhouseUsername, sinkClickhousePassword);
            ckStream.addSink(jdbcSink);
//        ckStream.print();
        }*/

        env.execute(appName);




/*        //sink:ck
        SingleOutputStreamOperator<FullMsg> ckStream = fullMsgStream.filter(msg -> !msg.isEmpty()).map(jsonObject -> {
                    JSONObject status = JSONObject.parseObject(jsonObject.getString("status"));
                    return FullMsg.of(jsonObject.getString("time"),
                            jsonObject.getString("institution"),
                            jsonObject.getString("system_code"),
                            jsonObject.getString("healthy_biz"),
                            jsonObject.getString("healthy_sys"),
                            status.getIntValue("RV"),
                            status.getDoubleValue("RS"),
                            status.getIntValue("RD"),
                            status.getDoubleValue("RR"),
                            jsonObject.getString("faults")
                    );
                }
        );

        //SINK Clickhouse
        String sql = "INSERT INTO default." + sinkClickhouseTable + " (time, institution, system_code,healthy_biz,healthy_sys,RV,RS,RD,RR,faults) VALUES (?,?,?,?,?,?,?,?,?,?)";
        J_MyClickHouseUtil2 jdbcSink = new J_MyClickHouseUtil2(sql, sinkClickhouseHost, sinkClickhousePort, sinkClickhouseDatabase, sinkClickhouseUsername, sinkClickhousePassword);
        ckStream.addSink(jdbcSink);
//        ckStream.print();*/

        //SINK kafka
//        CustomKafkaSerializationSchema customKafkaSerializationSchema = new CustomKafkaSerializationSchema("t_fac_counter");

        /*KafkaSink<String> sink = KafkaSink.<String>builder()
                .setKafkaProducerConfig(sinkProperties)
                .setBootstrapServers(sinkKafkaBootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("t_fac_counter")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();


        fullMsgStream.sinkTo(sink);
        fullMsgStream.print("00--------1");*/
/*        FlinkKafkaProducer<String> myProducer2 = new FlinkKafkaProducer<String>(
                "t_fac_counter",                  // 目标 topic
                new SimpleStringSchema(),     // 序列化 schema
                sinkProperties
//                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        ); */// 容错
//        SingleOutputStreamOperator<String> kafka1 = fullMsgStream.map(JSONAware::toJSONString);
//        fullMsgStream.addSink(myProducer2);
        /*fullMsgStream.filter(msg ->
            "BAE71E9B6CDE919FFE8F7193CA2987A4".equals(JSONObject.parseObject(msg).getString("system_code"))
        ).addSink(myProducer2);
        fullMsgStream.filter(msg ->
                "BAE71E9B6CDE919FFE8F7193CA2987A4".equals(JSONObject.parseObject(msg).getString("system_code"))
        ).print("t_fac_counter");*/


        //1
/*        String sysCode = systemCode[0];
        String sinkTopic = sinkKafkaTopic[0];
        KafkaSerializationSchema<String> serializationSchema = new KafkaSerializationSchema<String>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                return new ProducerRecord<>(
                        sinkTopic, // target topic
                        element.getBytes(StandardCharsets.UTF_8)); // record contents
            }
        };
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(
                sinkTopic,             // target topic
                serializationSchema,    // serialization schema
                sinkProperties,             // producer config
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE); // fault-tolerance

        SingleOutputStreamOperator<String> singleStream = fullMsgStream.filter(msg -> msg.getString("system_code").equals(sysCode))
                .map(JSONAware::toJSONString);
        singleStream.addSink(myProducer);*/


/*        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>(
                "",
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String str, @Nullable Long aLong) {
                        JSONObject jsonObject = JSONObject.parseObject(str);
                        String topic = jsonObject.getString("topic");
                        jsonObject.remove("topic");
                        return new ProducerRecord<byte[], byte[]>(topic, str.getBytes());
                    }
                },
                sinkProperties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE); //todo 这里是有EXACTLY_ONCE 是有问题的。
//                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
        fullMsgStream.addSink(producer);
        fullMsgStream.print("=================");*/

      /*  for (int i = 0; i < systemCode.length; i++) {
            //根据系统编码将不同的系统数据sink到对应的topic
            String sysCode = systemCode[i];
            String sinkTopic = sinkKafkaTopic[i];
            *//*KafkaSink<String> sink = KafkaSink.<String>builder()
                    .setKafkaProducerConfig(sinkProperties)
                    .setBootstrapServers(sinkKafkaBootstrapServers)
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic(sinkTopic)
                            .setValueSerializationSchema(new SimpleStringSchema())
                            .build()
                    )
                    .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    .build();*//*
//----------------------------------
            *//*SingleOutputStreamOperator<String> singleStream = fullMsgStream.filter(msg -> JSONObject.parseObject(msg).getString("system_code").equals(sysCode));
            singleStream.sinkTo(KafkaSink.<String>builder()
                    .setKafkaProducerConfig(sinkProperties)
                    .setBootstrapServers(sinkKafkaBootstrapServers)
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic(sinkTopic)
                            .setValueSerializationSchema(new SimpleStringSchema())
                            .build()
                    )
                    .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    .build());
            singleStream.print(sinkTopic);*//*

//--------------------------------------
            *//*KafkaSerializationSchema<String> serializationSchema = new KafkaSerializationSchema<String>() {
                @Override
                public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                    return new ProducerRecord<>(
                            sinkTopic, // target topic
                            element.getBytes(StandardCharsets.UTF_8)); // record contents
                }
            };
            FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(
                    sinkTopic,             // target topic
                    serializationSchema,    // serialization schema
                    sinkProperties,             // producer config
                    FlinkKafkaProducer.Semantic.EXACTLY_ONCE); // fault-tolerance

            SingleOutputStreamOperator<String> singleStream = fullMsgStream.filter(msg -> msg.getString("system_code").equals(sysCode))
                    .map(JSONAware::toJSONString);
            singleStream.addSink(myProducer);
            singleStream.print("kafka_"+i);
                    .addSink(new FlinkKafkaProducer<String>(
                            sinkTopic,                  // 目标 topic
                            new CustomKafkaSerializationSchema(sinkTopic),     // 序列化 schema
                            sinkProperties,
                            FlinkKafkaProducer.Semantic.EXACTLY_ONCE));*//*


        }


*//*
        fullMsgStream.filter(msg -> msg.getString("system_code").equals(systemCode[0])).addSink(new FlinkKafkaProducer<String>(
                sinkKafkaTopic,                  // 目标 topic
                new CustomKafkaSerializationSchema(sinkKafkaTopic);,     // 序列化 schema
        propertiesSink,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE); // 容错);
*//*

//        fullMsgStream.print("Kafka02");


        env.execute(appName);*/
    }

    //消息封装
    private static JSONObject messageEncapsulation(String msg, Map<String, String[]> sysCodeConfig, String institution, String triggerCondition) {
        JSONObject msgJson = JSONObject.parseObject(msg);
        JSONObject jsonObject = new JSONObject();
        String sysInfo = msgJson.getString("host") + msgJson.getString("spv") + msgJson.getString("intf");
        boolean isNeedSys = sysCodeConfig.containsKey(sysInfo);
        HashMap<String, Object> map = new HashMap<>();
        if (isNeedSys) {
            map.put("trans_count_total", msgJson.getDouble("trans_count_total"));
            map.put("succ_count_total", msgJson.getDouble("succ_count_total"));
            map.put("resp_count_total", msgJson.getDouble("resp_count_total"));
            map.put("duration_avg", msgJson.getDouble("duration_avg"));
            map.put("succ_p", msgJson.getDouble("succ_p"));
            map.put("resp_p", msgJson.getDouble("resp_p"));
            boolean isTrigger = (boolean) FilterUtils.convertToCode(triggerCondition, map);
            String time = DateUtil.StringToString(msgJson.getString("tsDate"), DateStyle.YYYYMMDDHHMMSS, DateStyle.YYYY_MM_DD_HH_MM);
            String systemCode = sysCodeConfig.get(sysInfo)[0];
            String topic = sysCodeConfig.get(sysInfo)[1];
            jsonObject.put("topic", topic);
            jsonObject.put("time", time);
            jsonObject.put("institution", institution);
            jsonObject.put("system_code", systemCode);
            if (isTrigger) {
                jsonObject.put("healthy_biz", "true");
            } else {
                jsonObject.put("healthy_biz", "false");
            }
            jsonObject.put("healthy_sys", "true");
            //业务运行状况描述
            JSONObject status = new JSONObject();
            //交易量 保留整数
            int RV = msgJson.getIntValue("trans_count_total");
            status.put("RV", RV);
            //成功率 <=100 四舍五入保留两位小数
            double RS = BigDecimal.valueOf(msgJson.getDoubleValue("succ_p") * 100).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
            status.put("RS", RS);
            //平均响应时间 *1000 单位：ms 保留整数
            int RD = (int) Math.round(msgJson.getDoubleValue("duration_avg") * 1000);
            status.put("RD", RD);
            //响应率 <=100 四舍五入保留两位小数
            double RR = BigDecimal.valueOf(msgJson.getDoubleValue("resp_p") * 100).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
            status.put("RR", RR);
            if (RV == 0 || RS == 0 || RD == 0 || RR == 0) {
                logger.warn("errorMSG:" + msg);
            }
            jsonObject.put("status", status);
            LinkedList<JSONObject> faults = new LinkedList<>();
            faults.add(new JSONObject());
            jsonObject.put("faults", faults);
//            jsonObject.put("sink_topic",sysCodeConfig.get(sysInfo)[1]);
        }

        return jsonObject;
    }

    //将这三个系统：xbank、分布式核心、统一平台的配置信息保存到map中，数据结构为key：host+spc+intf,value:(system_code,topic),用这个map来过滤kafka数据源中这三个系统的信息
    private static Map<String, String[]> sysCodeConfigBuilder(String[] system, String[] systemCode, String[] systemHost, String[] systemSpv, String[] systemIntf, String[] sinkKafkaTopic) {
        if (system.length != 0 && system.length == systemCode.length && systemCode.length == systemHost.length && systemHost.length == systemSpv.length && systemSpv.length == systemIntf.length) {
            Map<String, String[]> map = new HashMap<>();
            for (int i = 0; i < system.length; i++) {
                String key = systemHost[i] + systemSpv[i] + systemIntf[i];
                //保存系统编码、对应sink的topic
                String[] value = {systemCode[i], sinkKafkaTopic[i]};
                map.put(key, value);
            }
            return map;
        } else {
            return new HashMap<String, String[]>();
        }
    }

    private static KafkaSource<String> getKafkaSource(String brokers, String topics, String groupId, String offset) {
        if ("latest".equals(offset)) {
            return KafkaSource.<String>builder()
                    .setBootstrapServers(brokers)
                    .setTopics(topics)
                    .setGroupId(groupId)
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .build();

        }
        return KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

    }

    public static Properties getConsumerProperties(String sourceKafkaBootstrapServers, String sourceKafkaGroupId, String sourceKafkaOffset) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceKafkaBootstrapServers);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, sourceKafkaGroupId);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, sourceKafkaOffset);
        //消费者读取数据时候的隔离级别，设置为只读取已提交的数据
        properties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return properties;
    }

    public static Properties getProducerProperties(String sinkKafkaBootstrapServers) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, sinkKafkaBootstrapServers);
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "5");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "127000");
        //事务超时时间，因为flink的事务超时时间为1h，kafka的事务超时时间为15min，所以为了数据发送时一致，flink的超时时间应该小于等于kafka超时时间
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "900000");
        //是否开启幂等性
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        properties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "producer-" + new Random().nextInt(1000));


        return properties;
    }

    /*//加载配置文件
    public static void initProperties(String configPath, String user) {
        PropertiesUtil.initProperties(configPath, user);
        flinkVersion = PropertiesUtil.getDouble("flink_version");
        appName = PropertiesUtil.getString("app_name");
        checkpointInterval = PropertiesUtil.getInt("checkpoint_interval");
        checkpointDataUri = PropertiesUtil.getString("checkpointdatauri");
        parallelism = PropertiesUtil.getInt("parallelism");
        sourceKafkaBootstrapServers = PropertiesUtil.getString("source_kafka_bootstrap_servers");
        sourceKafkaGroupId = PropertiesUtil.getString("source_kafka_group_id");
        sourceKafkaTopic = PropertiesUtil.getString("source_kafka_topic");
        sourceKafkaOffset = PropertiesUtil.getString("source_kafka_offset");
        institution = PropertiesUtil.getString("institution");
        system = PropertiesUtil.getString("system").split(",");
        systemCode = PropertiesUtil.getString("system_code").replace(" ", "").split(",");
        systemHost = PropertiesUtil.getString("system_host").replace(" ", "").split(",");
        systemSpv = PropertiesUtil.getString("system_spv").replace(" ", "").split(",");
        systemIntf = PropertiesUtil.getString("system_intf").replace(" ", "").split(",");
        sinkKafkaTopic = PropertiesUtil.getString("sink_kafka_topic").replace(" ", "").split(",");
        triggerCondition = PropertiesUtil.getString("trigger_condition").replace(" ", "");
        sinkKafkaBootstrapServers = PropertiesUtil.getString("sink_kafka_bootstrap_servers");
        isSinkClickhouse = PropertiesUtil.getBoolean("is_sink_clickhouse");
        sinkClickhouseHost = PropertiesUtil.getString("sink_clickhouse_host");
        sinkClickhousePort = PropertiesUtil.getInt("sink_clickhouse_port");
        sinkClickhouseUsername = PropertiesUtil.getString("sink_clickhouse_username");
        sinkClickhousePassword = PropertiesUtil.getString("sink_clickhouse_password");
        sinkClickhouseDatabase = PropertiesUtil.getString("sink_clickhouse_database");
        sinkClickhouseTable = PropertiesUtil.getString("sink_clickhouse_table");
    }*/


}
