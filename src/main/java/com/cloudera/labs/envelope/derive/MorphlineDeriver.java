package com.cloudera.labs.envelope.derive;

import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.utils.MorphlineUtils;
import com.cloudera.labs.envelope.utils.RowUtils;
import com.typesafe.config.Config;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class MorphlineDeriver implements Deriver {

  private static final Logger LOG = LoggerFactory.getLogger(MorphlineDeriver.class);

  public static final String MORPHLINE = "morphline";
  public static final String MORPHLINE_ID = "morphline.id";
  public static final String PRODUCTION_MODE = "production.mode";
  public static final String FIELD_NAMES = "field.names";
  public static final String FIELD_TYPES = "field.types";

  private Config config;
  private StructType schema;
  private String morphlineFile;
  private String morphlineId;

  @Override
  public void configure(Config config) {
    LOG.trace("Configuring Morphline Deriver");

    this.config = config;

    // Set up the Morphline configuration, the file must be located on the local file system
    this.morphlineFile = config.getString(MORPHLINE);
    this.morphlineId = config.getString(MORPHLINE_ID);

    if (this.morphlineFile == null || this.morphlineFile.trim().length() == 0) {
      throw new MorphlineCompilationException("Missing or empty Morphline File configuration parameter", null);
    }

    // Construct the StructType schema for the Rows
    List<String> fieldNames = config.getStringList(FIELD_NAMES);
    List<String> fieldTypes = config.getStringList(FIELD_TYPES);
    this.schema = RowUtils.structTypeFor(fieldNames, fieldTypes);
  }

  @Override
  public DataFrame derive(Map<String, DataFrame> dependencies) throws Exception {
    LOG.debug("Executing on Dependencies {}", dependencies.keySet());

    // Get the DF
    if (dependencies.size() != 1) {
      throw new RuntimeException("MorphlineDeriver must have only one dependency");
    }
    DataFrame inputDF = dependencies.values().iterator().next();

    // For each partition in the DataFrame / RDD
    JavaRDD<Row> outputRDD = inputDF.toJavaRDD().flatMap(
        MorphlineUtils.morphlineMapper(this.morphlineFile, this.morphlineId, getSchema()));

    // Convert all the Rows into a new DataFrame
    return Contexts.getSQLContext().createDataFrame(outputRDD, getSchema());
  }

  /**
   *
   * @return The generated StructType for the resulting DataFrame
   */
  protected StructType getSchema() {
    return this.schema;
  }

}
