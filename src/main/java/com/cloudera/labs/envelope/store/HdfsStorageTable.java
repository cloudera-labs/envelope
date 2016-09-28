package com.cloudera.labs.envelope.store;

import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.plan.PlannedRecord;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * storage
 * storage.hdfs.path = HDFS FS path
 * storage.hdfs.type = avro, parquet [, text, sequence]
 * storage.hdfs.key
 * storage.hdfs.schema = Avro representation of underlying datum
 */
public class HdfsStorageTable extends StorageTable {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsStorageSystem.class);

  private FileSystem fileSystem;
  private Properties properties;
  private Path path;

  /**
   * http://blog.cloudera.com/blog/2014/05/how-to-convert-existing-data-into-parquet/
   * https://cwiki.apache.org/confluence/display/Hive/HCatalog+ReaderWriter
   */
  public HdfsStorageTable(FileSystem fileSystem, Properties properties) {
    this.fileSystem = fileSystem;
    this.properties = properties;
    this.path = new Path(properties.getProperty("hdfs.path"));
  }

  /**
   * The mutation types that the storage table is able to apply.
   *
   * @return The set of supported mutation types.
   */
  @Override
  public Set<MutationType> getSupportedMutationTypes() {
    return Sets.newHashSet(MutationType.INSERT);
  }

  /**
   * The schema of the storage table.
   *
   * @return The Apache Avro schema equivalent for the schema of the storage table.
   */
  @Override
  public Schema getSchema() {
    return null;
  }

  /**
   * Get the existing records in the storage table for the given filter.
   *
   * @param filter The record whose field names and values are used as an equality filter on the storage table.
   * @return The list of storage records in the table schema that match the filter.
   */
  @Override
  public List<GenericRecord> getExistingForFilter(GenericRecord filter) throws Exception {
    throw new RuntimeException("HDFS storage does not yet permit retrieval for filtering");
  }

  /**
   * Apply the planned mutations to the storage table.
   *
   * @param mutations The list of planned mutations.
   */
  @Override
  public void applyPlannedMutations(List<PlannedRecord> mutations) throws Exception {
    String type = this.properties.getProperty("hdfs.type", "avro");
    Schema schema = mutations.get(0).getSchema();

    switch (type) {
      case "avro" :
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(writer);
        fileWriter.setMeta("Envelope", "Yeah yeah");

        Path file = new Path(this.path, createUUID(type));
        fileWriter.create(schema, this.fileSystem.create(file, true));

        LOG.info("Writing to Avro container: {}", file.toString());

        for (PlannedRecord planned : mutations) {
          fileWriter.append(planned);
        }

        fileWriter.close();
        break;
      default:
        LOG.error("Unrecognized storage.hdfs.type: {}", type);
    }
  }

  private String createUUID(String suffix) {
    return UUID.randomUUID().toString() + "." + suffix;
  }
}
