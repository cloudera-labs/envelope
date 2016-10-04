package com.cloudera.labs.envelope.source;

import com.cloudera.labs.envelope.translate.Translator;
import java.util.Properties;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 *
 */
public class HdfsBulkSource extends BulkSource {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsBulkSource.class);

  public HdfsBulkSource(Properties props) {
    super(props);
  }

  @Override
  public JavaRDD<GenericRecord> rddFor(JavaSparkContext jsc, final Properties props) {

    String path = props.getProperty("source.hdfs.path");
    String type = props.getProperty("source.hdfs.type");
    int partitions = 0;


    if (props.containsKey("source.hdfs.partitions")) {
     partitions = Integer.parseInt(props.getProperty("source.hdfs.partitions"));
    }

    Configuration configuration = new Configuration();

    JavaRDD<GenericRecord> output;

    switch (type) {
      case "text" :
        JavaRDD<String> text;
        if (partitions != 0) {
          text = jsc.textFile(path, partitions);
        } else {
          text = jsc.textFile(path);
        }

        output = text.map(new Function<String, GenericRecord>() {
          Translator<Void, String> translator;

          @Override
          public GenericRecord call(String line) throws Exception {
            if (translator == null) {
              translator = Translator.translatorFor(Void.class, String.class, props);
            }

            return translator.translate(line);
          }
        });
        break;
      case "avro" :
        AvroKeyInputFormat<GenericRecord> inputFormat = new AvroKeyInputFormat<>();

        JavaPairRDD<AvroKey, Void> avro = jsc.newAPIHadoopFile(
            path,
            inputFormat.getClass(),
            AvroKey.class,
            Void.class,
            configuration
        );

        if (partitions != 0) {
          avro = avro.repartition(partitions);
        }

        output = avro.map(new Function<Tuple2<AvroKey, Void>, GenericRecord>() {
          Translator<Void, GenericRecord> translator;

          @Override
          public GenericRecord call(Tuple2<AvroKey, Void> wrapper) throws Exception {
            if (translator == null) {
              translator = Translator.translatorFor(Void.class, GenericRecord.class, props);
            }

            return translator.translate((GenericRecord) wrapper._1().datum());
          }

        });
        break;
      default :
        LOG.error("Unrecognized 'source.hdfs.type': {}", type);
        output = null;
    }

    return output;
  }

}
