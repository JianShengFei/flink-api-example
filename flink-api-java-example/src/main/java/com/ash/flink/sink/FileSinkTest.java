package com.ash.flink.sink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

public class FileSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.socketTextStream("localhost", 9999);

        FileSink<String> fileSink = FileSink.forRowFormat(new Path("./output/java-file-path"),
                        new SimpleStringEncoder<String>("UTF-8"))
                //生成新桶目录的检查周期，默认1分钟
                .withBucketCheckInterval(1000)
                //设置文件滚动策略
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        //桶不活跃的间隔时长，默认1分钟
                        .withInactivityInterval(Duration.ofSeconds(30))
                        //设置文件多大后生成新的文件，默认128M
                        .withMaxPartSize(MemorySize.ofMebiBytes(1024))
                        //设置每隔多长时间生成一个新的文件，默认1分钟
                        .withRolloverInterval(Duration.ofSeconds(10))
                        .build())
                .build();

        ds.sinkTo(fileSink);
        env.execute();
    }


}
