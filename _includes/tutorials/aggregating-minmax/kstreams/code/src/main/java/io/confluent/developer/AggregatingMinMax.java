package io.confluent.developer;

import io.confluent.developer.avro.MovieTicketSales;
import io.confluent.developer.avro.YearlyMovieFigures;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class AggregatingMinMax {

  public Properties buildStreamsProperties(Properties envProps) {
    Properties props = new Properties();

    props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("application.id"));
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty("schema.registry.url"));
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

    return props;
  }

  private SpecificAvroSerde<MovieTicketSales> ticketSaleSerde(final Properties envProps) {
    final SpecificAvroSerde<MovieTicketSales> serde = new SpecificAvroSerde<>();
    Map<String, String> config = new HashMap<>();
    config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty("schema.registry.url"));
    serde.configure(config, false);
    return serde;
  }
  private SpecificAvroSerde<YearlyMovieFigures> movieFiguresSerde(final Properties envProps) {
    final SpecificAvroSerde<YearlyMovieFigures> serde = new SpecificAvroSerde<>();
    Map<String, String> config = new HashMap<>();
    config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty("schema.registry.url"));
    serde.configure(config, false);
    return serde;
  }

  public Topology buildTopology(Properties envProps,
                                final SpecificAvroSerde<MovieTicketSales> ticketSaleSerde,
                                final SpecificAvroSerde<YearlyMovieFigures> movieFiguresSerde) {
    final StreamsBuilder builder = new StreamsBuilder();

    final String inputTopic = envProps.getProperty("input.topic.name");
    final String outputTopic = envProps.getProperty("output.topic.name");

    // These two topologies will produce records with no serialization errors
    //builder.stream(inputTopic, Consumed.with(Serdes.String(), ticketSaleSerde))
    //    .groupBy( (k, v) -> v.getReleaseYear(), Grouped.with(Serdes.Integer(), ticketSaleSerde))
    //    .reduce(((value1, value2) -> value2), Materialized.with(Serdes.Integer(), ticketSaleSerde))
    //    .toStream()
    //    .map( ((key, value) -> new KeyValue(key, new YearlyMovieFigures(key, 1, 2))))
    //    .to(outputTopic, Produced.with(Serdes.Integer(), movieFiguresSerde));

    //builder.stream(inputTopic, Consumed.with(Serdes.String(), ticketSaleSerde))
    //        .groupBy(
    //                (key, value) -> value.getTitle(),
    //                Grouped.with(Serdes.String(), ticketSaleSerde))
    //        .aggregate(
    //                () -> 0L,
    //                ((key, value, aggregate) -> aggregate + value.getTitle().length()),
    //                Materialized.with(Serdes.String(), Serdes.Long()))
    //        .toStream()
    //        .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

    // this topology results in Serialization Exceptions
    //      (Caused by: org.apache.kafka.common.errors.SerializationException: Error deserializing Avro message for id -1
    // Having something to do with the usage of the `YearlyMovieFigures` type in the initializer
    builder.stream(inputTopic, Consumed.with(Serdes.String(), ticketSaleSerde))
         .groupBy(
                 (k, v) -> v.getReleaseYear(),
                 Grouped.with(Serdes.Integer(), ticketSaleSerde))
         .aggregate(
                 () -> new YearlyMovieFigures(),
                 ((key, value, aggregate) -> aggregate ),
                 Materialized.with(Serdes.Integer(), movieFiguresSerde))
         .toStream()
         .to(outputTopic, Produced.with(Serdes.Integer(), movieFiguresSerde));

    return builder.build();
  }

  public void createTopics(Properties envProps) {
    Map<String, Object> config = new HashMap<>();
    config.put("bootstrap.servers", envProps.getProperty("bootstrap.servers"));
    AdminClient client = AdminClient.create(config);

    List<NewTopic> topics = new ArrayList<>();
    topics.add(new NewTopic(
        envProps.getProperty("input.topic.name"),
        Integer.parseInt(envProps.getProperty("input.topic.partitions")),
        Short.parseShort(envProps.getProperty("input.topic.replication.factor"))));
    topics.add(new NewTopic(
        envProps.getProperty("output.topic.name"),
        Integer.parseInt(envProps.getProperty("output.topic.partitions")),
        Short.parseShort(envProps.getProperty("output.topic.replication.factor"))));

    client.createTopics(topics);
    client.close();
  }

  public Properties loadEnvProperties(String fileName) throws IOException {
    Properties envProps = new Properties();
    FileInputStream input = new FileInputStream(fileName);
    envProps.load(input);
    input.close();

    return envProps;
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      throw new IllegalArgumentException(
          "This program takes one argument: the path to an environment configuration file.");
    }

    new AggregatingMinMax().runRecipe(args[0]);
  }

  private void runRecipe(final String configPath) throws IOException {
    Properties envProps = this.loadEnvProperties(configPath);
    Properties streamProps = this.buildStreamsProperties(envProps);

    Topology topology = this.buildTopology(envProps,
            this.ticketSaleSerde(envProps),
            this.movieFiguresSerde(envProps));

    this.createTopics(envProps);

    final KafkaStreams streams = new KafkaStreams(topology, streamProps);
    final CountDownLatch latch = new CountDownLatch(1);

    // Attach shutdown handler to catch Control-C.
    Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
      @Override
      public void run() {
        streams.close();
        latch.countDown();
      }
    });

    try {
      streams.start();
      latch.await();
    } catch (Throwable e) {
      System.exit(1);
    }
    System.exit(0);

  }
}
