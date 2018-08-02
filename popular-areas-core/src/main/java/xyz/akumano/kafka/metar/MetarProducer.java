package xyz.akumano.kafka.metar;

import org.apache.commons.cli.*;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.serialization.StringSerializer;
import xyz.akumano.kafka.common.CsvProducer;

import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import java.util.Scanner;

/**
 * keys are null.
 */
public class MetarProducer extends CsvProducer<String, Metar> {
    public MetarProducer(String topicName, String cliendId, String bootstrapServer) {
        super(topicName, cliendId, bootstrapServer,
                StringSerializer.class, Metar.MetarSerializer.class);
    }
    protected String getKey(Metar metar) {
        return null;
    }

    private class MetarIterator implements Iterator<Metar> {

        private Scanner sc;
        MetarIterator(Scanner sc) {
            // Grab the header, get it out of the way.
            String header = sc.nextLine();
            //String[] columns = header.split(", ");
            this.sc = sc;
        }

        @Override
        public Metar next() {
            String[] row = sc.nextLine().split(",");
            Metar metar = new Metar(row[1], Double.valueOf(row[2]), Double.valueOf(row[4]));
            return metar;
        }

        @Override
        public boolean hasNext() {
            return sc.hasNext();
        }
    }
    protected Iterator<Metar> recordIterator(Scanner sc) {
        return new MetarIterator(sc);
    }

    public static void main( String[] args ) throws Exception
    {
        Options options = new Options();
        Option topicNameOption = Option.builder()
                .longOpt("topicName")
                .hasArg().required()
                .desc("The name of a topic to which metar data is sent.")
                .build(),
        csvPathOption = Option.builder()
                .longOpt("csvPath")
                .hasArg().required()
                .desc("The uri of a csv file containing the metar data to send.")
                .build(),
        bootstrapServerOption = Option.builder()
                .longOpt("bootstrapServer")
                .hasArg().required()
                .desc("The uri of a Kafka bootstrap server.")
                .build();

        options.addOption(topicNameOption);
        options.addOption(csvPathOption);
        options.addOption(bootstrapServerOption);

        if (args.length == 0) {
            HelpFormatter help = new HelpFormatter();
            help.printHelp("MetarProducer", options);
        }


        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String topicName = cmd.getOptionValue(topicNameOption);
        //String topicName = "metar-2014-05";

        String clientId = "metar-producer-local";
        String csvPath = cmd.getOptionValue(csvPathOption);
        //String csvPath = "/lga-2014-05-metar.csv";

        String bootstrapServer = cmd.getOptionValue(bootstrapServerOption);
        //String bootstrapServer = "localhost:9092";

        // clear topic before adding
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        AdminClient admin = AdminClient.create(props);
        admin.deleteTopics(Collections.singletonList(topicName));

        MetarProducer producer = new MetarProducer(topicName, clientId, bootstrapServer);
        producer.send(csvPath);
        producer.close();
    }
}
