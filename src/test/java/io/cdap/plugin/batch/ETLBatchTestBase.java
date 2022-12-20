/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.batch;

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.batch.source.ftp.FTPBatchSource;
import io.cdap.plugin.format.avro.input.AvroInputFormatProvider;
import io.cdap.plugin.format.avro.output.AvroOutputFormatProvider;
import io.cdap.plugin.format.blob.input.BlobInputFormatProvider;
import io.cdap.plugin.format.delimited.input.CSVInputFormatProvider;
import io.cdap.plugin.format.delimited.input.DelimitedInputFormatProvider;
import io.cdap.plugin.format.delimited.input.TSVInputFormatProvider;
import io.cdap.plugin.format.delimited.output.CSVOutputFormatProvider;
import io.cdap.plugin.format.delimited.output.DelimitedOutputFormatProvider;
import io.cdap.plugin.format.delimited.output.TSVOutputFormatProvider;
import io.cdap.plugin.format.json.input.JsonInputFormatProvider;
import io.cdap.plugin.format.json.output.JsonOutputFormatProvider;
import io.cdap.plugin.format.orc.output.OrcOutputFormatProvider;
import io.cdap.plugin.format.parquet.input.ParquetInputFormatProvider;
import io.cdap.plugin.format.parquet.output.ParquetOutputFormatProvider;
import io.cdap.plugin.format.text.input.TextInputFormatProvider;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcOutputFormat;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.xerial.snappy.Snappy;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Base test class that sets up plugin and the etl batch app artifacts.
 */
public class ETLBatchTestBase extends HydratorTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", true);

  protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("data-pipeline", "3.2.0");
  protected static final ArtifactSummary DATAPIPELINE_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");

  private static int startCount;

  @BeforeClass
  public static void setupTest() throws Exception {
    if (startCount++ > 0) {
      return;
    }

    setupBatchArtifacts(DATAPIPELINE_ARTIFACT_ID, DataPipelineApp.class);

    Set<ArtifactRange> parents = ImmutableSet.of(
      new ArtifactRange(NamespaceId.DEFAULT.getNamespace(), DATAPIPELINE_ARTIFACT_ID.getArtifact(),
                        new ArtifactVersion(DATAPIPELINE_ARTIFACT_ID.getVersion()), true,
                        new ArtifactVersion(DATAPIPELINE_ARTIFACT_ID.getVersion()), true)
    );
    // add artifact for batch sources and sinks
    addPluginArtifact(NamespaceId.DEFAULT.artifact("ftp-plugins", "1.0.0"), parents,
                      FTPBatchSource.class
    );
    // add format plugins
    addPluginArtifact(NamespaceId.DEFAULT.artifact("formats-avro", "4.0.0"), DATAPIPELINE_ARTIFACT_ID,
                      ImmutableSet.of(AvroOutputFormatProvider.PLUGIN_CLASS, AvroInputFormatProvider.PLUGIN_CLASS),
                      AvroOutputFormatProvider.class, AvroInputFormatProvider.class);
    addPluginArtifact(NamespaceId.DEFAULT.artifact("formats-blob", "4.0.0"), DATAPIPELINE_ARTIFACT_ID,
                      ImmutableSet.of(BlobInputFormatProvider.PLUGIN_CLASS), BlobInputFormatProvider.class);
    addPluginArtifact(NamespaceId.DEFAULT.artifact("formats-delimited", "4.0.0"), DATAPIPELINE_ARTIFACT_ID,
                      ImmutableSet.of(DelimitedOutputFormatProvider.PLUGIN_CLASS,
                                      DelimitedInputFormatProvider.PLUGIN_CLASS,
                                      CSVOutputFormatProvider.PLUGIN_CLASS, CSVInputFormatProvider.PLUGIN_CLASS,
                                      TSVOutputFormatProvider.PLUGIN_CLASS, TSVInputFormatProvider.PLUGIN_CLASS),
                      DelimitedOutputFormatProvider.class, DelimitedInputFormatProvider.class);
    addPluginArtifact(NamespaceId.DEFAULT.artifact("formats-json", "4.0.0"), DATAPIPELINE_ARTIFACT_ID,
                      ImmutableSet.of(JsonOutputFormatProvider.PLUGIN_CLASS, JsonInputFormatProvider.PLUGIN_CLASS),
                      JsonOutputFormatProvider.class, JsonInputFormatProvider.class);
    addPluginArtifact(NamespaceId.DEFAULT.artifact("formats-orc", "4.0.0"), DATAPIPELINE_ARTIFACT_ID,
                      ImmutableSet.of(OrcOutputFormatProvider.PLUGIN_CLASS),
                      OrcOutputFormatProvider.class, OrcOutputFormat.class, OrcStruct.class,
                      TypeDescription.class, TimestampColumnVector.class);
    addPluginArtifact(NamespaceId.DEFAULT.artifact("formats-parquet", "4.0.0"), DATAPIPELINE_ARTIFACT_ID,
                      ImmutableSet.of(ParquetOutputFormatProvider.PLUGIN_CLASS,
                                      ParquetInputFormatProvider.PLUGIN_CLASS),
                      ParquetOutputFormatProvider.class, ParquetInputFormatProvider.class,
                      Snappy.class);
    addPluginArtifact(NamespaceId.DEFAULT.artifact("formats-text", "4.0.0"), DATAPIPELINE_ARTIFACT_ID,
                      ImmutableSet.of(TextInputFormatProvider.PLUGIN_CLASS), TextInputFormatProvider.class);
  }

  protected ApplicationManager deployETL(ETLBatchConfig etlConfig, String appName) throws Exception {
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(appName);
    return deployApplication(appId, appRequest);
  }

  /**
   * Run the SmartWorkflow in the given ETL application for once and wait for the workflow's COMPLETED status
   * with 5 minutes timeout.
   *
   * @param appManager the ETL application to run
   * @param arguments  the arguments to be passed when running SmartWorkflow
   */
  protected WorkflowManager runETLOnce(ApplicationManager appManager, Map<String, String> arguments)
    throws TimeoutException, InterruptedException, ExecutionException {
    final WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    int numRuns = workflowManager.getHistory().size();
    workflowManager.start(arguments);
    Tasks.waitFor(numRuns + 1, () -> workflowManager.getHistory().size(), 20, TimeUnit.SECONDS);
    workflowManager.waitForStopped(5, TimeUnit.MINUTES);
    return workflowManager;
  }
}
