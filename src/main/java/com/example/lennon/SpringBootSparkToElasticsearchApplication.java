package com.example.lennon;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.ImmutableList;
import org.spark_project.guava.collect.ImmutableMap;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

/**
 * Created by lennon on 3/23/2017 AD.
 */

@SpringBootApplication
@ConfigurationProperties(prefix = "spark.es")
public class SpringBootSparkToElasticsearchApplication implements ApplicationRunner {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(SpringBootSparkToElasticsearchApplication.class);

    private static final String NODES = "spark.es.nodes";
    private static final String PORT = "spark.es.port";
    private static final String RESOURCE = "spark.es.resource";
    private static final String INDEX_AUTO_CREATE = "spark.es.index.auto.create";

    private String nodes;
    private String port;
    private String resource;
    private String indexAutoCreate;

    public static void main(String[] args) {
        SpringApplication.run(SpringBootSparkToElasticsearchApplication.class, args);
    }

    @Override
    public void run(ApplicationArguments applicationArguments) throws Exception {

        LOGGER.info("SpringBootSpark To Elasticsearch Application: {}, {}, {}, {}", nodes, port, resource, indexAutoCreate);

        String master = "local[*]";

        SparkConf conf = new SparkConf()
                .setAppName(SpringBootSparkToElasticsearchApplication.class.getName())
                .setMaster(master);

        conf.set(NODES, nodes);
        conf.set(PORT, port);
        conf.set(RESOURCE, resource);
        conf.set(INDEX_AUTO_CREATE, indexAutoCreate);

        JavaSparkContext context = new JavaSparkContext(conf);

        Map<String, ?> nameOne = ImmutableMap.of("name", "one");
        Map<String, ?> nameTwo = ImmutableMap.of("name", "two");

        JavaRDD<Map<String, ?>> javaRDD = context.parallelize(ImmutableList.of(nameOne, nameTwo));
        JavaEsSpark.saveToEs(javaRDD, resource);

    }

    public String getNodes() {
        return nodes;
    }

    public void setNodes(String nodes) {
        this.nodes = nodes;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getResource() {
        return resource;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }

    public String getIndexAutoCreate() {
        return indexAutoCreate;
    }

    public void setIndexAutoCreate(String indexAutoCreate) {
        this.indexAutoCreate = indexAutoCreate;
    }
}
