package io.confluent.developer;

import io.confluent.developer.avro.MovieTicketSales;
import io.confluent.developer.avro.YearlyMovieFigures;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.time.Year;
import java.util.*;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import static java.util.Arrays.asList;

public class AggregatingMinMaxTest {

  private final static String TEST_CONFIG_FILE = "configuration/test.properties";

  private static SpecificAvroSerde<MovieTicketSales> makeMovieTicketSalesSerializer(
          Properties envProps, SchemaRegistryClient srClient) throws IOException, RestClientException {

    SpecificAvroSerde<MovieTicketSales> serde = new SpecificAvroSerde<>(srClient);

    Map<String, String> config = new HashMap<>();
    config.put("schema.registry.url", envProps.getProperty("schema.registry.url"));
    serde.configure(config, false);

    return serde;
  }
  private static SpecificAvroSerde<YearlyMovieFigures> makeYearlyMovieFiguresSerializer(
          Properties envProps, SchemaRegistryClient srClient) throws IOException, RestClientException {

    SpecificAvroSerde<YearlyMovieFigures> serde = new SpecificAvroSerde<>(srClient);

    Map<String, String> config = new HashMap<>();
    config.put("schema.registry.url", envProps.getProperty("schema.registry.url"));
    serde.configure(config, false);

    return serde;
  }

  @Test
  public void shouldCountTicketSales() throws IOException, RestClientException {

    AggregatingMinMax minMaxStream = new AggregatingMinMax();
    Properties envProps = minMaxStream.loadEnvProperties(TEST_CONFIG_FILE);
    Properties streamProps = minMaxStream.buildStreamsProperties(envProps);

    final MockSchemaRegistryClient srClient = new MockSchemaRegistryClient();

    String inputTopic = envProps.getProperty("input.topic.name");
    String outputTopic = envProps.getProperty("output.topic.name");

    srClient.register(inputTopic + "-value", MovieTicketSales.SCHEMA$);
    //srClient.register(outputTopic + "-value", YearlyMovieFigures.SCHEMA$);

    final SpecificAvroSerde<MovieTicketSales> movieTicketSerdes =
            makeMovieTicketSalesSerializer(envProps, srClient);
    final SpecificAvroSerde<YearlyMovieFigures> yearlyFiguresSerdes =
            makeYearlyMovieFiguresSerializer(envProps, srClient);
    Topology topology = minMaxStream.buildTopology(envProps, movieTicketSerdes, yearlyFiguresSerdes);

    TopologyTestDriver testDriver = new TopologyTestDriver(topology, streamProps);

    ConsumerRecordFactory<String, MovieTicketSales>
        inputFactory =
        new ConsumerRecordFactory<>(Serdes.String().serializer(), movieTicketSerdes.serializer());

    final List<MovieTicketSales>
        input = asList(
          new MovieTicketSales("Avengers: Endgame", 2019, 856980506),
          new MovieTicketSales("Captain Marvel", 2019, 426829839),
          new MovieTicketSales("Toy Story 4", 2019, 401486230),
          new MovieTicketSales("The Lion King", 2019, 385082142),
          new MovieTicketSales("Black Panther", 2018, 700059566),
          new MovieTicketSales("Avengers: Infinity War", 2018, 678815482),
          new MovieTicketSales("Deadpool 2", 2018, 324512774),
          new MovieTicketSales("Beauty and the Beast", 2017, 517218368),
          new MovieTicketSales("Wonder Woman", 2017, 412563408),
          new MovieTicketSales("Star Wars Ep. VIII: The Last Jedi", 2017, 517218368)
        );

    input.forEach( record -> testDriver.pipeInput(inputFactory.create(inputTopic, "", record)));

    List<Long> actualOutput = new ArrayList<>();
    boolean stop = false;

    do {
      stop = Optional.ofNullable(testDriver.readOutput(
                outputTopic,
                Serdes.String().deserializer(),
                Serdes.Long().deserializer()))
             .map(record -> record.value())
             .map(movieFigures -> {
               actualOutput.add(movieFigures);
               return false;
             })
             .orElse(true);
    } while (!stop);

    System.out.println(actualOutput);

    //List<Long> expectedOutput = new ArrayList<Long>(Arrays.asList(1L, 2L, 1L, 3L, 2L, 1L, 2L, 3L, 4L));
    //Assert.assertEquals(expectedOutput, actualOutput);

  }

}
