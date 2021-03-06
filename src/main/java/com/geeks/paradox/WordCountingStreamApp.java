package com.geeks.paradox;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.geeks.paradox.models.Word;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

public class WordCountingStreamApp {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("WordCountingStreamApp");
        sparkConf.set("spark.cassandra.connection.host", "127.0.0.1");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        Collection<String> topics = Collections.singletonList("messages");
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, getKafkaParams())
        );

        JavaPairDStream<String, String> results = messages.mapToPair(r -> new Tuple2<>(r.key(), r.value()));
        JavaDStream<String> lines = results.map(Tuple2::_2);
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split("\\s+")).iterator());
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey(Integer::sum);

        wordCounts.foreachRDD(javaRDD -> {
            Map<String, Integer> wordCountMap = javaRDD.collectAsMap();
            for (String key: wordCountMap.keySet()) {
                List<Word> wordList = Collections.singletonList(new Word(key, wordCountMap.get(key)));
                JavaRDD<Word> rdd = streamingContext.sparkContext().parallelize(wordList);
                CassandraJavaUtil.javaFunctions(rdd)
                        .writerBuilder("vocabulary", "words", CassandraJavaUtil.mapToRow(Word.class))
                        .saveToCassandra();
            }
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }

    private static Map<String, Object> getKafkaParams() {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "1");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        return kafkaParams;
    }
}
