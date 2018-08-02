package xyz.akumano.kafka.prediction;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.cli.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.FieldValue;
import org.jpmml.evaluator.InputField;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.jpmml.model.PMMLUtil;
import scala.Tuple2;
import xyz.akumano.beam.popularareas.CellCount;
import xyz.akumano.beam.popularareas.KafkaUtils;
import xyz.akumano.kafka.metar.Metar;
import xyz.akumano.util.GeoUtils;

import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

// Warning: the number of records produced is large, since, for each metar record, it makes NGRID_X * NGRID_Y = 250*400 = 100k predictions.
// It took 30 minutes to process the 2014-05 data up to "2014-05-01 05:29", which is the 14th row. That's 2 minutes per row.
public class PopularityPredictor {
    private static PMML importModel(String path) throws Exception {
        InputStream in = PopularityPredictor.class.getResourceAsStream(path);
        return PMMLUtil.unmarshal(in);
    }

    /**
     * TODO: use only the data from the 51st minute of an hour.
     * Fields are "cell_index_x", "cell_index_y", "weekday", "hour", "temperature", and "precipitation".
     * Any of these features could be missing if it is not used for splitting in the random forest.
     * Splits are:
     * - temperature: [40, 55, 70, 85]
     * - precipitation: [0.1, 0.3]
     * @param metar
     * @param inputFields
     * @return
     */
    private static List<Tuple2<String, Map<FieldName, FieldValue>>> prepArgumentsForOneMetar(Metar metar, List<InputField> inputFields) {
        //System.out.println("\n\n" + metar + "\n\n");
        List<Tuple2<String, Map<FieldName, FieldValue>>> args = Lists.newArrayList();
        InputField xIndexInputField = findField("cell_index_x", inputFields);
        InputField yIndexInputField = findField("cell_index_y", inputFields);
        // create a cross product of cell_indices and the fields of the given metar intance
        Tuple2<String, Map<FieldName, FieldValue>> metarArguemntsWithDatetime = prepMetarArgumentsWithDatetime(metar, inputFields);
        Map<FieldName, FieldValue> fullArguemnts;
        for (int id = 0; id < GeoUtils.NGRID_X()*GeoUtils.NGRID_Y(); id++) {
            fullArguemnts = Maps.newHashMap(metarArguemntsWithDatetime._2());
            Tuple2<Object, Object> index = GeoUtils.getIndex(id);
            int xIndex = (int) index._1();
            int yIndex = (int) index._2();
            FieldValue xIndexFieldValue = xIndexInputField.prepare(xIndex);
            FieldValue yIndexFieldValue = yIndexInputField.prepare(yIndex);
            fullArguemnts.put(xIndexInputField.getName(), xIndexFieldValue);
            fullArguemnts.put(yIndexInputField.getName(), yIndexFieldValue);
            args.add(new Tuple2<>(metarArguemntsWithDatetime._1(), fullArguemnts));
        }
        return args;
    }
    private static InputField findField(String name, List<InputField> inputFields) {
        for (InputField field : inputFields) {
            if (name.equals(field.getName().getValue()))
                return field;
        }
        throw new IllegalArgumentException("There is no field with name = " + name + ".");
    }
    // prepare fields from a Metar instance
    private static Tuple2<String, Map<FieldName, FieldValue>> prepMetarArgumentsWithDatetime(Metar metar, List<InputField> inputFields) {
        Map<FieldName, FieldValue> args = Maps.newHashMap();
        Map<String, ?> metarFields = getMetarFields(metar);
        for (InputField field : inputFields) {
            FieldName fieldName = field.getName();
            // get fields from the Metar instance
            String name = fieldName.getValue();
            if (!(name.equals("cell_index_x") || name.equals("cell_index_y"))) {
                FieldValue fieldValue = field.prepare(metarFields.get(name));
                args.put(fieldName, fieldValue);
            }
        }
        return new Tuple2<>(metar.getDatetime(), args);
    }
    // a map from an InputField name to a field of a Metar instance
    private static Map<String, Object> getMetarFields(Metar metar) {
        Map<String, Object> fields = Maps.newHashMap();
        LocalDateTime datetime = LocalDateTime.parse(metar.getDatetime(),
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
        fields.put("hour", datetime.getHour());
        fields.put("weekday", datetime.getDayOfWeek().getValue());
        fields.put("precipitation", bucketPrecip(metar.getPrecipIn()));
        fields.put("temperature", bucketTmpf(metar.getFahrenheit()));
        return fields;
    }


    // bucket a precipitation value in inch
    private static Integer bucketPrecip(double precip) {
        if (precip < 0.1) return 0;
        else if (precip < 0.3) return 1;
        else return 2;
    }
    // bucket fahrenheit
    private static Integer bucketTmpf(double fahrenheit) {
        if (fahrenheit < 40) return 0;
        else if (fahrenheit < 55) return 1;
        else if (fahrenheit < 70) return 2;
        else if (fahrenheit < 85) return 3;
        else return 4;
    }

    private static ValueMapper prepareArgs(List<InputField> inputFields) {

        final List<InputField> fields = inputFields;
        return new ValueMapper<Metar, Iterable<Tuple2<String, Map<FieldName, FieldValue>>>>() {
            @Override
            public Iterable<Tuple2<String, Map<FieldName, FieldValue>>> apply(Metar metar) {
                return prepArgumentsForOneMetar(metar, fields);
            }
        };
    }

    private static KeyValueMapper countsPrediction(Evaluator modelEvaluator) {

        final Evaluator evaluator = modelEvaluator;
        return new KeyValueMapper<String, Tuple2<String, Map<FieldName, FieldValue>>,
                KeyValue<Integer,CellCount>>() {
            @Override
            public KeyValue<Integer, CellCount> apply(String key, Tuple2<String, Map<FieldName, FieldValue>> argsWithDatetime) {
                String datetime = argsWithDatetime._1();
                Map<FieldName, FieldValue> args = argsWithDatetime._2();
                Object countPrediction = evaluator
                        .evaluate(args)
                        .get(new FieldName("count"));
                int cellIndexX = (int) args.get(new FieldName("cell_index_x")).getValue();
                int cellIndexY = (int) args.get(new FieldName("cell_index_y")).getValue();
                int cell_id = GeoUtils.NGRID_X()*cellIndexY + cellIndexX;
                return new KeyValue<>((int) cell_id,
                        new CellCount(datetime, (int) cell_id, (long) Math.round((double) countPrediction)));
            }
        };
    }

    public static void main(String[] args) throws Exception {

        Options options = new Options();
        Option sourceTopicNameOption = Option.builder()
                .longOpt("sourceTopicName")
                .hasArg().required()
                .desc("The name of a topic from which metar data is read.")
                .build(),
                sinkTopicNameOption = Option.builder()
                        .longOpt("sinkTopicName")
                        .hasArg().required()
                        .desc("The name of a topic to which prediction is written.")
                        .build(),
                bootstrapServerOption = Option.builder()
                        .longOpt("bootstrapServer")
                        .hasArg().required()
                        .desc("The uri of a Kafka bootstrap server.")
                        .build();

        options.addOption(sourceTopicNameOption);
        options.addOption(sinkTopicNameOption);
        options.addOption(bootstrapServerOption);

        if (args.length == 0) {
            HelpFormatter help = new HelpFormatter();
            help.printHelp("PopularityPredictor", options);
        }


        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String sourceTopicName = cmd.getOptionValue(sourceTopicNameOption);
        String sinkTopicName = cmd.getOptionValue(sinkTopicNameOption);
        String bootstrapServer = cmd.getOptionValue(bootstrapServerOption);
        //String sourceTopicName = "metar-2014-05";
        //String sinkTopicName = "prediction-2014-05";
        //String bootstrapServer = "localhost:9092";

        String appId = "popularity-prediction-kstreams";

        KafkaUtils.clearTopic(sinkTopicName, bootstrapServer);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Metar> source = builder.stream(
                sourceTopicName, Consumed.with(Serdes.String(), Metar.MetarSerde));

        // TODO: dynamic plugging of a model
        PMML rfModel = importModel("/rfModel-201405.pmml");
        Evaluator modelEvaluator = ModelEvaluatorFactory.newInstance().newModelEvaluator(rfModel);

        List<InputField> inputFields = modelEvaluator.getInputFields();
        //System.out.println(inputFields);

        // map Metar instances to arguments of the random forest model.
        KStream<String, Tuple2<String, Map<FieldName, FieldValue>>> arguments = source.flatMapValues(prepareArgs(inputFields) );
        // make prediction, and store it with the cell_id as key
        KStream<Integer, CellCount> prediction = arguments.map(countsPrediction(modelEvaluator));

        //source.to("prediction-2014-05");
        prediction.to(sinkTopicName,
                Produced.with(Serdes.Integer(), CellCount.CellCountSerde));

        final Topology topology = builder.build();
        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);

        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
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
