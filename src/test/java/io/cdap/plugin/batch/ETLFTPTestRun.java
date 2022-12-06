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

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.WorkflowManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * FTP Source Test.
 */
public class ETLFTPTestRun extends ETLBatchTestBase {
  private static final String USER = "ftp";
  private static final String PWD = "abcd";
  private static final String TEST_STRING = "Hello World";
  private static final String TEST_STRING_2 = "Goodnight Moon";
  private static File folder;
  private static File file2;
  private static int port;
  private static FakeFtpServer ftpServer;
  private static final String PATH = "path";
  private static final String IGNORE_NON_EXISTING_FOLDERS = "ignoreNonExistingFolders";
  private static final String FILE_REGEX = "fileRegex";

  @Before
  public void init() throws Exception {
    folder = TMP_FOLDER.newFolder();
    File file = new File(folder, "sample");
    file2 = new File(folder, "sample2");

    ftpServer = new FakeFtpServer();
    ftpServer.setServerControlPort(0);

    FileSystem fileSystem = new UnixFakeFileSystem();
    fileSystem.add(new FileEntry(file.getAbsolutePath(), TEST_STRING));
    fileSystem.add(new FileEntry(file2.getAbsolutePath(), TEST_STRING_2));
    ftpServer.setFileSystem(fileSystem);

    ftpServer.addUserAccount(new UserAccount(USER, PWD, folder.getAbsolutePath()));
    ftpServer.start();

    Tasks.waitFor(true, () -> ftpServer.isStarted(), 5, TimeUnit.SECONDS);
    port = ftpServer.getServerControlPort();
  }

  @After
  public void stop() {
    if (ftpServer != null) {
      ftpServer.stop();
    }
  }

  @Test
  public void testFTPBatchSource() throws Exception {
    ETLStage source = new ETLStage("source", new ETLPlugin(
      "FTP",
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put(PATH, String.format("ftp://%s:%s@localhost:%d%s",
                                 USER, PWD, port, folder.getAbsolutePath()))
        .put(IGNORE_NON_EXISTING_FOLDERS, "false")
        .put("referenceName", "ftp")
        .build(),
      null));

    String outputDatasetName = "testing-ftp-source";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("FTPBatchSource");

    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 30, TimeUnit.SECONDS);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    Assert.assertEquals("Expected records", 2, output.size());
    Set<String> outputValue = new HashSet<>();
    for (StructuredRecord record : output) {
      outputValue.add(record.get("body"));
    }
    Assert.assertTrue(outputValue.contains(TEST_STRING));
    Assert.assertTrue(outputValue.contains(TEST_STRING_2));
  }

  @Test
  public void testFTPBatchSourceWithRegex() throws Exception {
    ETLStage source = new ETLStage("source", new ETLPlugin(
      "FTP",
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put(PATH, String.format("ftp://%s:%s@localhost:%d%s",
                                 USER, PWD, port, folder.getAbsolutePath()))
        .put(IGNORE_NON_EXISTING_FOLDERS, "false")
        .put(FILE_REGEX, file2.getAbsolutePath())
        .put("referenceName", "ftp")
        .build(),
      null));

    String outputDatasetName = "testing-ftp-source-with-regex";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("FTPBatchSource");

    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 30, TimeUnit.SECONDS);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    Assert.assertEquals("Expected records", 1, output.size());
    Assert.assertEquals("Single file", TEST_STRING_2, output.get(0).get("body"));
  }

  private void testHelper(Map<String, String> properties, Map<String, String> runTimeProperties) throws Exception {
    ETLStage source = new ETLStage("source", new ETLPlugin(
      "FTP", BatchSource.PLUGIN_TYPE, properties, null));

    String outputDatasetName = "testing-ftp-source-with-macro";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    ApplicationManager appManager = deployETL(etlConfig, "FTPWithMacro");
    runETLOnce(appManager, runTimeProperties);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    Assert.assertEquals("Expected records", 2, output.size());
    Set<String> outputValue = new HashSet<>();
    for (StructuredRecord record : output) {
      outputValue.add(record.get("body"));
    }
    Assert.assertTrue(outputValue.contains("Hello World"));
  }

  @Test
  public void testFTPBatchSourceWithMacro() throws Exception {
    testHelper(ImmutableMap.of("path", "${path}", "referenceName", "${referenceName}"),
               ImmutableMap.of("path", String.format("ftp://%s:%s@localhost:%d%s",
                                                     USER, PWD, port, folder.getAbsolutePath()),
                               "referenceName", "ftp_with_macro"));
  }

  @Test
  public void testFTPBatchSourceWithInvalidPath() {
    ETLStage source = new ETLStage("source", new ETLPlugin(
      "FTP",
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put(PATH, String.format("ftp://localhost:%d%s",
                                 port, folder.getAbsolutePath()))
        .put(IGNORE_NON_EXISTING_FOLDERS, "false")
        .put("referenceName", "ftp")
        .build(),
      null));

    String outputDatasetName = "testing-ftp-source";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("FTPBatchSourceWithInvalidPath");

    // deploying would thrown an exception
    try {
      deployApplication(appId, appRequest);
      Assert.fail();
    } catch (Exception e) {
      //
    }
  }
}
