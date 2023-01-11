/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.plugin.batch.source.ftp;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.batch.JobUtils;
import io.cdap.plugin.format.FileFormat;
import io.cdap.plugin.format.plugin.AbstractFileSource;
import io.cdap.plugin.format.plugin.FileSourceProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * {@link BatchSource} that reads from an FTP or SFTP server.
 */
@Plugin(type = "batchsource")
@Name("FTP")
@Description("Batch source for an FTP or SFTP source. Prefix of the path ('ftp://...' or 'sftp://...') determines " +
  "the source server type, either FTP or SFTP.")
public class FTPBatchSource extends AbstractFileSource {
  public static final Logger LOG = LoggerFactory.getLogger(FTPBatchSource.class);
  private static final String NAME_FILE_SYSTEM_PROPERTIES = "fileSystemProperties";
  private static final String FS_SFTP_IMPL = "fs.sftp.impl";
  private static final String SFTP_FS_CLASS = FTPBatchSource.SFTPFileSystem.class.getName();
  private static final String FTP_PROTOCOL = "ftp";
  private static final String SFTP_PROTOCOL = "sftp";
  private static final String PATH = "path";
  private static final int DEFAULT_FTP_PORT = 21;
  private static final int DEFAULT_SFTP_PORT = 22;

  public static final Schema SCHEMA = Schema.recordOf("text",
                                                      Schema.Field.of("offset", Schema.of(Schema.Type.LONG)),
                                                      Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
  private static final Pattern PATTERN_WITHOUT_SPECIAL_CHARACTERS = Pattern.compile("[^A-Za-z0-9]");
  private final FTPBatchSourceConfig config;
  // private Asset asset;

  public FTPBatchSource(FTPBatchSourceConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    // create asset for lineage
    // String referenceName = Strings.isNullOrEmpty(config.getReferenceName())
    //   ? ReferenceNames.normalizeFqn(config.getPath())
    //   : config.getReferenceName();
    // asset = Asset.builder(referenceName)
    //   .setFqn(config.getPath()).build();

    // super is called down here to avoid instantiating the lineage recorder with a null asset
    super.prepareRun(context);
  }

  @Override
  protected Map<String, String> getFileSystemProperties(BatchSourceContext context) {
    Map<String, String> properties = new HashMap<>();
    if (context != null) {
      properties.putAll(config.getFileSystemProperties(context.getFailureCollector()));
    }

    if (!properties.containsKey(FS_SFTP_IMPL)) {
      properties.put(FS_SFTP_IMPL, SFTP_FS_CLASS);
    }
    // Limit the number of splits to 1 since FTPInputStream does not support seek;
    properties.put(FileInputFormat.SPLIT_MINSIZE, Long.toString(Long.MAX_VALUE));
    return properties;
  }

  // @Override
  // protected LineageRecorder getLineageRecorder(BatchSourceContext context) {
  //   return new LineageRecorder(context, asset);
  // }

  /**
   * Config class that contains all the properties needed for FTP Batch Source.
   */
  @SuppressWarnings("unused")
  public static class FTPBatchSourceConfig extends PluginConfig implements FileSourceProperties {
    private static final Gson GSON = new Gson();
    private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() {
    }.getType();

    @Macro
    @Nullable
    @Description("Name be used to uniquely identify this source for lineage, annotating metadata, etc.")
    private String referenceName;

    @Macro
    @Description("Path to file(s) to be read. Path is expected to be of the form " +
      "'prefix://username:password@hostname:port/path'.")
    private String path;

    @Macro
    @Nullable
    @Description("Any additional properties to use when reading from the filesystem. "
      + "This is an advanced feature that requires knowledge of the properties supported by the underlying filesystem.")
    private String fileSystemProperties;

    @Nullable
    @Description("Whether to allow an input that does not exist. When false, the source will fail the run if the input "
      + "does not exist. When true, the run will not fail and the source will not generate any output. "
      + "The default value is false.")
    private Boolean ignoreNonExistingFolders;

    @Macro
    @Nullable
    @Description("Regular expression that file names must match in order to be read. "
      + "If no value is given, no file filtering will be done. "
      + "See https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html for more information about "
      + "the regular expression syntax.")
    private String fileRegex;

    @Override
    public void validate(FailureCollector collector) {
      try {
        if (containsMacro(PATH)) {
          return;
        }
        validateAuthenticationInPath(collector);

        Map<String, String> fsp = getFileSystemProperties(collector);
        try {
          Path urlInfo;
          String extractedPassword = extractPasswordFromUrl();
          String encodedPassword = URLEncoder.encode(extractedPassword);
          String validatePath = path.replace(extractedPassword, encodedPassword);
          try {
            urlInfo = new Path(validatePath);
          } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Unable to parse url: %s %s", e.getMessage(), e));
          }
          Job job = JobUtils.createInstance();
          Configuration conf = job.getConfiguration();
          for (Map.Entry<String, String> entry : fsp.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
          }
          String protocol = urlInfo.toUri().getScheme();
          if (protocol.equals(SFTP_PROTOCOL)) {
            conf.set(FS_SFTP_IMPL, SFTP_FS_CLASS);
          }
          JobUtils.applyWithExtraClassLoader(job, getClass().getClassLoader(),
              f -> {
                FileSystem fs = FileSystem.get(urlInfo.toUri(), conf);
                // TODO: Add setTimeout option in the future
                // https://cdap.atlassian.net/browse/PLUGIN-1181
                return fs.getFileStatus(urlInfo);
              });
        } catch (Exception e) {
          //Log exception details as otherwise we loose it, and it's hard to debug
          LOG.warn("Unable to connect with the given url", e);
          collector.addFailure("Unable to connect with given url", null)
            .withConfigProperty(PATH).withStacktrace(e.getStackTrace());
        }
      } catch (IllegalArgumentException e) {
        collector.addFailure("File system properties must be a valid json.", null)
          .withConfigProperty(NAME_FILE_SYSTEM_PROPERTIES).withStacktrace(e.getStackTrace());
      }
    }

    @Override
    public String getReferenceName() {
      return referenceName;
    }

    @Override
    public String getPath() {
      if (authContainsSpecialCharacters()) {
        Path urlInfo;
        String extractedPassword = extractPasswordFromUrl();
        String encodedPassword = URLEncoder.encode(extractedPassword);
        String validatePath = path.replace(extractedPassword, encodedPassword);
        try {
          urlInfo = new Path(validatePath);
        } catch (Exception e) {
          throw new IllegalArgumentException(String.format("Unable to parse url: %s %s", e.getMessage(), e));
        }
        String host = urlInfo.toUri().getAuthority().substring(urlInfo.toUri().getAuthority().lastIndexOf("@") + 1);
        String user = urlInfo.toUri().getAuthority().split(":")[0];
        String protocol = urlInfo.toUri().getScheme();
        int port = urlInfo.toUri().getPort();
        if (port == -1 && protocol.equals(FTP_PROTOCOL)) {
          port = DEFAULT_FTP_PORT;
        }
        if (port == -1 && protocol.equals(SFTP_PROTOCOL)) {
          port = DEFAULT_SFTP_PORT;
        }
        String cleanHost = host.replace(":" + port, "");
        return urlInfo.toUri().getScheme() + "://" + cleanHost + urlInfo.toUri().getPath();
      }
      return path;
    }

    @Override
    public String getFormatName() {
      return FileFormat.TEXT.name().toLowerCase();
    }

    @Nullable
    @Override
    public Pattern getFilePattern() {
      return Strings.isNullOrEmpty(fileRegex) ? null : Pattern.compile(fileRegex);
    }

    @Override
    public long getMaxSplitSize() {
      return Long.MAX_VALUE;
    }

    @Override
    public boolean shouldAllowEmptyInput() {
      return false;
    }

    @Override
    public boolean shouldReadRecursively() {
      return false;
    }

    @Nullable
    @Override
    public String getPathField() {
      return null;
    }

    @Override
    public boolean useFilenameAsPath() {
      return false;
    }

    @Override
    public boolean skipHeader() {
      return false;
    }

    @Nullable
    @Override
    public Schema getSchema() {
      return SCHEMA;
    }

    @VisibleForTesting
    void configuration(String path, String fileSystemProperties) {
      this.path = path;
      this.fileSystemProperties = fileSystemProperties;
    }

    @VisibleForTesting
    String getPathFromConfig() {
      return path;
    }

    /**
     * This method extracts the password from url
     */
    public String extractPasswordFromUrl() {
      int getLastIndexOfAtSign = path.lastIndexOf("@");
      String authentication = path.substring(0, getLastIndexOfAtSign);
      return authentication.substring(authentication.lastIndexOf(":") + 1);
    }

    /**
     * This method checks if extracted password from url has any special characters
     */
    public boolean authContainsSpecialCharacters() {
      Matcher specialCharacters = PATTERN_WITHOUT_SPECIAL_CHARACTERS.matcher(extractPasswordFromUrl());
      return specialCharacters.find();
    }

    Map<String, String> getFileSystemProperties(FailureCollector collector) {
      HashMap<String, String> fileSystemPropertiesMap = new HashMap<>();
      if (!Strings.isNullOrEmpty(fileSystemProperties)) {
        fileSystemPropertiesMap.putAll(GSON.fromJson(fileSystemProperties, MAP_STRING_STRING_TYPE));
      }

      if (!containsMacro(PATH) && authContainsSpecialCharacters()) {
        Path urlInfo = null;
        String extractedPassword = extractPasswordFromUrl();
        String encodedPassword = URLEncoder.encode(extractedPassword);
        String validatePath = path.replace(extractedPassword, encodedPassword);
        try {
          urlInfo = new Path(validatePath);
        } catch (Exception e) {
          if (collector != null) {
            collector.addFailure(String.format("Unable to parse url: %s.", path),
                                 "Path is expected to be of the form " +
                                   "prefix://username:password@hostname:port/path")
              .withConfigProperty(PATH).withStacktrace(e.getStackTrace());
            collector.getOrThrowException();
          } else {
            throw new IllegalArgumentException(String.format("Unable to parse url: %s. %s", path,
                                                             "Path is expected to be of the form " +
                                                               "prefix://username:password@hostname:port/path"));
          }
        }
        // After encoding the url, the format should look like:
        // ftp://kimi:42%4067%5Dgfuss@192.168.0.179:21/kimi-look-here.txt
        int port = urlInfo.toUri().getPort();
        String host = urlInfo.toUri().getAuthority().substring(urlInfo.toUri().getAuthority().lastIndexOf("@") + 1);
        String user = urlInfo.toUri().getAuthority().split(":")[0];
        if (urlInfo.toUri().getScheme().equals(FTP_PROTOCOL)) {
          port = (port == -1) ? DEFAULT_FTP_PORT : port;
          String cleanHostFTP = host.replace(":" + port, "");
          fileSystemPropertiesMap.put("fs.ftp.host", cleanHostFTP);
          fileSystemPropertiesMap.put(String.format("fs.ftp.user.%s", cleanHostFTP), user);
          fileSystemPropertiesMap.put(String.format("fs.ftp.password.%s", cleanHostFTP), extractedPassword);
          fileSystemPropertiesMap.put("fs.ftp.host.port", String.valueOf(port));
        } else {
          port = (port == -1) ? DEFAULT_SFTP_PORT : port;
          String cleanHostSFTP = host.replace(":" + port, "");
          fileSystemPropertiesMap.put("fs.sftp.host", cleanHostSFTP);
          fileSystemPropertiesMap.put(String.format("fs.sftp.user.%s", cleanHostSFTP), user);
          fileSystemPropertiesMap.put(String.format("fs.sftp.password.%s.%s", cleanHostSFTP, user),
                                      extractedPassword);
          fileSystemPropertiesMap.put("fs.sftp.host.port", String.valueOf(port));
        }
      }
      return fileSystemPropertiesMap;
    }

    /**
     * This method checks if the path contains authentication and it's in a valid form
     * If the user doesn't provide the authentication in path, we will raise a failure
     * Example of missing authentication in url: ftp://192.168.0.179/kimi-look-here.txt
     */
    private void validateAuthenticationInPath(FailureCollector collector) {
      int getLastIndexOfAtSign = path.lastIndexOf("@");
      if (getLastIndexOfAtSign == -1) {
        collector.addFailure(String.format("Missing authentication in url: %s.", path),
                             "Path is expected to be of the form " +
                               "prefix://username:password@hostname:port/path")
          .withConfigProperty(PATH);
        collector.getOrThrowException();
      }
    }
  }

  /** SFTP FileSystem.
   * This class and its dependency classes were copied from hadoop-2.8 to support SFTP FileSystem.
   * It's present in hadoop, but not fully available in Spark and has
   * https://issues.apache.org/jira/browse/HADOOP-18090 problem in hadoop3, so it's safer to have
   * own copy of it.
   * */
  static class SFTPFileSystem extends FileSystem {

    private FTPBatchSource.SFTPConnectionPool connectionPool;
    private URI uri;

    private static final int DEFAULT_SFTP_PORT = 22;
    private static final int DEFAULT_MAX_CONNECTION = 5;
    public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
    public static final int DEFAULT_BLOCK_SIZE = 4 * 1024;
    public static final String FS_SFTP_USER_PREFIX = "fs.sftp.user.";
    public static final String FS_SFTP_PASSWORD_PREFIX = "fs.sftp.password.";
    public static final String FS_SFTP_HOST = "fs.sftp.host";
    public static final String FS_SFTP_HOST_PORT = "fs.sftp.host.port";
    public static final String FS_SFTP_KEYFILE = "fs.sftp.keyfile";
    public static final String FS_SFTP_CONNECTION_MAX = "fs.sftp.connection.max";
    public static final String E_SAME_DIRECTORY_ONLY =
      "only same directory renames are supported";
    public static final String E_HOST_NULL = "Invalid host specified";
    public static final String E_USER_NULL =
      "No user specified for sftp connection. Expand URI or credential file.";
    public static final String E_PATH_DIR = "Path %s is a directory.";
    public static final String E_FILE_STATUS = "Failed to get file status";
    public static final String E_FILE_NOTFOUND = "File %s does not exist.";
    public static final String E_FILE_EXIST = "File already exists: %s";
    public static final String E_CREATE_DIR =
      "create(): Mkdirs failed to create: %s";
    public static final String E_DIR_CREATE_FROMFILE =
      "Can't make directory for path %s since it is a file.";
    public static final String E_MAKE_DIR_FORPATH =
      "Can't make directory for path \"%s\" under \"%s\".";
    public static final String E_DIR_NOTEMPTY = "Directory: %s is not empty.";
    public static final String E_FILE_CHECK_FAILED = "File check failed";
    public static final String E_NOT_SUPPORTED = "Not supported";
    public static final String E_SPATH_NOTEXIST = "Source path %s does not exist";
    public static final String E_DPATH_EXIST =
      "Destination path %s already exist, cannot rename!";
    public static final String E_FAILED_GETHOME = "Failed to get home directory";
    public static final String E_FAILED_DISCONNECT = "Failed to disconnect";

    /**
     * Set configuration from UI.
     *
     * @param uri
     * @param conf
     * @throws IOException
     */
    private void setConfigurationFromURI(URI uriInfo, Configuration conf)
      throws IOException {

      // get host information from URI
      String host = uriInfo.getHost();
      host = (host == null) ? conf.get(FS_SFTP_HOST, null) : host;
      if (host == null) {
        throw new IOException(E_HOST_NULL);
      }
      conf.set(FS_SFTP_HOST, host);

      int port = uriInfo.getPort();
      port = (port == -1)
        ? conf.getInt(FS_SFTP_HOST_PORT, DEFAULT_SFTP_PORT)
        : port;
      conf.setInt(FS_SFTP_HOST_PORT, port);

      // get user/password information from URI
      String userAndPwdFromUri = uriInfo.getUserInfo();
      if (userAndPwdFromUri != null) {
        String[] userPasswdInfo = userAndPwdFromUri.split(":");
        String user = userPasswdInfo[0];
        user = URLDecoder.decode(user, "UTF-8");
        conf.set(FS_SFTP_USER_PREFIX + host, user);
        if (userPasswdInfo.length > 1) {
          conf.set(FS_SFTP_PASSWORD_PREFIX + host + "." +
                     user, userPasswdInfo[1]);
        }
      }

      String user = conf.get(FS_SFTP_USER_PREFIX + host);
      if (user == null || user.equals("")) {
        throw new IllegalStateException(E_USER_NULL);
      }

      int connectionMax =
        conf.getInt(FS_SFTP_CONNECTION_MAX, DEFAULT_MAX_CONNECTION);
      connectionPool = new FTPBatchSource.SFTPConnectionPool(connectionMax);
    }

    /**
     * Connecting by using configuration parameters.
     *
     * @return An FTPClient instance
     * @throws IOException
     */
    private ChannelSftp connect() throws IOException {
      Configuration conf = getConf();

      String host = conf.get(FS_SFTP_HOST, null);
      int port = conf.getInt(FS_SFTP_HOST_PORT, DEFAULT_SFTP_PORT);
      String user = conf.get(FS_SFTP_USER_PREFIX + host, null);
      String pwd = conf.get(FS_SFTP_PASSWORD_PREFIX + host + "." + user, null);
      String keyFile = conf.get(FS_SFTP_KEYFILE, null);

      ChannelSftp channel =
        connectionPool.connect(host, port, user, pwd, keyFile);

      return channel;
    }

    /**
     * Logout and disconnect the given channel.
     *
     * @param client
     * @throws IOException
     */
    private void disconnect(ChannelSftp channel) throws IOException {
      connectionPool.disconnect(channel);
    }

    /**
     * Resolve against given working directory.
     *
     * @param workDir
     * @param path
     * @return absolute path
     */
    private Path makeAbsolute(Path workDir, Path path) {
      if (path.isAbsolute()) {
        return path;
      }
      return new Path(workDir, path);
    }

    /**
     * Convenience method, so that we don't open a new connection when using this
     * method from within another method. Otherwise every API invocation incurs
     * the overhead of opening/closing a TCP connection.
     * @throws IOException
     */
    private boolean exists(ChannelSftp channel, Path file) throws IOException {
      try {
        getFileStatus(channel, file);
        return true;
      } catch (FileNotFoundException fnfe) {
        return false;
      } catch (IOException ioe) {
        throw new IOException(E_FILE_STATUS, ioe);
      }
    }

    /**
     * Convenience method, so that we don't open a new connection when using this
     * method from within another method. Otherwise every API invocation incurs
     * the overhead of opening/closing a TCP connection.
     */
    @SuppressWarnings("unchecked")
    private FileStatus getFileStatus(ChannelSftp client, Path file)
      throws IOException {
      FileStatus fileStat = null;
      Path workDir;
      try {
        workDir = new Path(client.pwd());
      } catch (SftpException e) {
        throw new IOException(e);
      }
      Path absolute = makeAbsolute(workDir, file);
      Path parentPath = absolute.getParent();
      if (parentPath == null) { // root directory
        long length = -1; // Length of root directory on server not known
        boolean isDir = true;
        int blockReplication = 1;
        long blockSize = DEFAULT_BLOCK_SIZE; // Block Size not known.
        long modTime = -1; // Modification time of root directory not known.
        Path root = new Path("/");
        return new FileStatus(length, isDir, blockReplication, blockSize,
                              modTime,
                              root.makeQualified(this.getUri(), this.getWorkingDirectory()));
      }
      String pathName = parentPath.toUri().getPath();
      Vector<LsEntry> sftpFiles;
      try {
        sftpFiles = (Vector<LsEntry>) client.ls(pathName);
      } catch (SftpException e) {
        throw new FileNotFoundException(String.format(E_FILE_NOTFOUND, file));
      }
      if (sftpFiles != null) {
        for (LsEntry sftpFile : sftpFiles) {
          if (sftpFile.getFilename().equals(file.getName())) {
            // file found in directory
            fileStat = getFileStatus(client, sftpFile, parentPath);
            break;
          }
        }
        if (fileStat == null) {
          throw new FileNotFoundException(String.format(E_FILE_NOTFOUND, file));
        }
      } else {
        throw new FileNotFoundException(String.format(E_FILE_NOTFOUND, file));
      }
      return fileStat;
    }

    /**
     * Convert the file information in LsEntry to a {@link FileStatus} object. *
     *
     * @param sftpFile
     * @param parentPath
     * @return file status
     * @throws IOException
     */
    private FileStatus getFileStatus(ChannelSftp channel, LsEntry sftpFile,
                                     Path parentPath) throws IOException {

      SftpATTRS attr = sftpFile.getAttrs();
      long length = attr.getSize();
      boolean isDir = attr.isDir();
      boolean isLink = attr.isLink();
      if (isLink) {
        String link = parentPath.toUri().getPath() + "/" + sftpFile.getFilename();
        try {
          link = channel.realpath(link);

          Path linkParent = new Path("/", link);

          FileStatus fstat = getFileStatus(channel, linkParent);
          isDir = fstat.isDirectory();
          length = fstat.getLen();
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
      int blockReplication = 1;
      // Using default block size since there is no way in SFTP channel to know of
      // block sizes on server. The assumption could be less than ideal.
      long blockSize = DEFAULT_BLOCK_SIZE;
      long modTime = attr.getMTime() * 1000; // convert to milliseconds
      long accessTime = 0;
      FsPermission permission = getPermissions(sftpFile);
      // not be able to get the real user group name, just use the user and group
      // id
      String user = Integer.toString(attr.getUId());
      String group = Integer.toString(attr.getGId());
      Path filePath = new Path(parentPath, sftpFile.getFilename());

      return new FileStatus(length, isDir, blockReplication, blockSize, modTime,
                            accessTime, permission, user, group, filePath.makeQualified(
        this.getUri(), this.getWorkingDirectory()));
    }

    /**
     * Return file permission.
     *
     * @param sftpFile
     * @return file permission
     */
    private FsPermission getPermissions(LsEntry sftpFile) {
      return new FsPermission((short) sftpFile.getAttrs().getPermissions());
    }

    /**
     * Convenience method, so that we don't open a new connection when using this
     * method from within another method. Otherwise every API invocation incurs
     * the overhead of opening/closing a TCP connection.
     */
    private boolean mkdirs(ChannelSftp client, Path file, FsPermission permission)
      throws IOException {
      boolean created = true;
      Path workDir;
      try {
        workDir = new Path(client.pwd());
      } catch (SftpException e) {
        throw new IOException(e);
      }
      Path absolute = makeAbsolute(workDir, file);
      String pathName = absolute.getName();
      if (!exists(client, absolute)) {
        Path parent = absolute.getParent();
        created =
          (parent == null || mkdirs(client, parent, FsPermission.getDefault()));
        if (created) {
          String parentDir = parent.toUri().getPath();
          boolean succeeded = true;
          try {
            client.cd(parentDir);
            client.mkdir(pathName);
          } catch (SftpException e) {
            throw new IOException(String.format(E_MAKE_DIR_FORPATH, pathName,
                                                parentDir));
          }
          created = created & succeeded;
        }
      } else if (isFile(client, absolute)) {
        throw new IOException(String.format(E_DIR_CREATE_FROMFILE, absolute));
      }
      return created;
    }

    /**
     * Convenience method, so that we don't open a new connection when using this
     * method from within another method. Otherwise every API invocation incurs
     * the overhead of opening/closing a TCP connection.
     * @throws IOException
     */
    private boolean isFile(ChannelSftp channel, Path file) throws IOException {
      try {
        return !getFileStatus(channel, file).isDirectory();
      } catch (FileNotFoundException e) {
        return false; // file does not exist
      } catch (IOException ioe) {
        throw new IOException(E_FILE_CHECK_FAILED, ioe);
      }
    }

    /**
     * Convenience method, so that we don't open a new connection when using this
     * method from within another method. Otherwise every API invocation incurs
     * the overhead of opening/closing a TCP connection.
     */
    private boolean delete(ChannelSftp channel, Path file, boolean recursive)
      throws IOException {
      Path workDir;
      try {
        workDir = new Path(channel.pwd());
      } catch (SftpException e) {
        throw new IOException(e);
      }
      Path absolute = makeAbsolute(workDir, file);
      String pathName = absolute.toUri().getPath();
      FileStatus fileStat = null;
      try {
        fileStat = getFileStatus(channel, absolute);
      } catch (FileNotFoundException e) {
        // file not found, no need to delete, return true
        return false;
      }
      if (!fileStat.isDirectory()) {
        boolean status = true;
        try {
          channel.rm(pathName);
        } catch (SftpException e) {
          status = false;
        }
        return status;
      } else {
        boolean status = true;
        FileStatus[] dirEntries = listStatus(channel, absolute);
        if (dirEntries != null && dirEntries.length > 0) {
          if (!recursive) {
            throw new IOException(String.format(E_DIR_NOTEMPTY, file));
          }
          for (int i = 0; i < dirEntries.length; ++i) {
            delete(channel, new Path(absolute, dirEntries[i].getPath()),
                   recursive);
          }
        }
        try {
          channel.rmdir(pathName);
        } catch (SftpException e) {
          status = false;
        }
        return status;
      }
    }

    /**
     * Convenience method, so that we don't open a new connection when using this
     * method from within another method. Otherwise every API invocation incurs
     * the overhead of opening/closing a TCP connection.
     */
    @SuppressWarnings("unchecked")
    private FileStatus[] listStatus(ChannelSftp client, Path file)
      throws IOException {
      Path workDir;
      try {
        workDir = new Path(client.pwd());
      } catch (SftpException e) {
        throw new IOException(e);
      }
      Path absolute = makeAbsolute(workDir, file);
      FileStatus fileStat = getFileStatus(client, absolute);
      if (!fileStat.isDirectory()) {
        return new FileStatus[] {fileStat};
      }
      Vector<LsEntry> sftpFiles;
      try {
        sftpFiles = (Vector<LsEntry>) client.ls(absolute.toUri().getPath());
      } catch (SftpException e) {
        throw new IOException(e);
      }
      ArrayList<FileStatus> fileStats = new ArrayList<FileStatus>();
      for (int i = 0; i < sftpFiles.size(); i++) {
        LsEntry entry = sftpFiles.get(i);
        String fname = entry.getFilename();
        // skip current and parent directory, ie. "." and ".."
        if (!".".equalsIgnoreCase(fname) && !"..".equalsIgnoreCase(fname)) {
          fileStats.add(getFileStatus(client, entry, absolute));
        }
      }
      return fileStats.toArray(new FileStatus[fileStats.size()]);
    }

    /**
     * Convenience method, so that we don't open a new connection when using this
     * method from within another method. Otherwise every API invocation incurs
     * the overhead of opening/closing a TCP connection.
     *
     * @param channel
     * @param src
     * @param dst
     * @return rename successful?
     * @throws IOException
     */
    private boolean rename(ChannelSftp channel, Path src, Path dst)
      throws IOException {
      Path workDir;
      try {
        workDir = new Path(channel.pwd());
      } catch (SftpException e) {
        throw new IOException(e);
      }
      Path absoluteSrc = makeAbsolute(workDir, src);
      Path absoluteDst = makeAbsolute(workDir, dst);

      if (!exists(channel, absoluteSrc)) {
        throw new IOException(String.format(E_SPATH_NOTEXIST, src));
      }
      if (exists(channel, absoluteDst)) {
        throw new IOException(String.format(E_DPATH_EXIST, dst));
      }
      boolean renamed = true;
      try {
        channel.cd("/");
        channel.rename(src.toUri().getPath(), dst.toUri().getPath());
      } catch (SftpException e) {
        renamed = false;
      }
      return renamed;
    }

    @Override
    public void initialize(URI uriInfo, Configuration conf) throws IOException {
      super.initialize(uriInfo, conf);

      setConfigurationFromURI(uriInfo, conf);
      setConf(conf);
      this.uri = uriInfo;
    }

    @Override
    public String getScheme() {
      return "sftp";
    }

    @Override
    public URI getUri() {
      return uri;
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
      ChannelSftp channel = connect();
      Path workDir;
      try {
        workDir = new Path(channel.pwd());
      } catch (SftpException e) {
        throw new IOException(e);
      }
      Path absolute = makeAbsolute(workDir, f);
      FileStatus fileStat = getFileStatus(channel, absolute);
      if (fileStat.isDirectory()) {
        disconnect(channel);
        throw new IOException(String.format(E_PATH_DIR, f));
      }
      InputStream is;
      try {
        // the path could be a symbolic link, so get the real path
        absolute = new Path("/", channel.realpath(absolute.toUri().getPath()));

        is = channel.get(absolute.toUri().getPath());
      } catch (SftpException e) {
        throw new IOException(e);
      }

      FSDataInputStream fis =
        new FSDataInputStream(new FTPBatchSource.SFTPInputStream(is, channel, statistics));
      return fis;
    }

    /**
     * A stream obtained via this call must be closed before using other APIs of
     * this class or else the invocation will block.
     */
    @Override
    public FSDataOutputStream create(Path f, FsPermission permission,
                                     boolean overwrite, int bufferSize, short replication, long blockSize,
                                     Progressable progress) throws IOException {
      final ChannelSftp client = connect();
      Path workDir;
      try {
        workDir = new Path(client.pwd());
      } catch (SftpException e) {
        throw new IOException(e);
      }
      Path absolute = makeAbsolute(workDir, f);
      if (exists(client, f)) {
        if (overwrite) {
          delete(client, f, false);
        } else {
          disconnect(client);
          throw new IOException(String.format(E_FILE_EXIST, f));
        }
      }
      Path parent = absolute.getParent();
      if (parent == null || !mkdirs(client, parent, FsPermission.getDefault())) {
        parent = (parent == null) ? new Path("/") : parent;
        disconnect(client);
        throw new IOException(String.format(E_CREATE_DIR, parent));
      }
      OutputStream os;
      try {
        client.cd(parent.toUri().getPath());
        os = client.put(f.getName());
      } catch (SftpException e) {
        throw new IOException(e);
      }
      FSDataOutputStream fos = new FSDataOutputStream(os, statistics) {
        @Override
        public void close() throws IOException {
          super.close();
          disconnect(client);
        }
      };

      return fos;
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize,
                                     Progressable progress)
      throws IOException {
      throw new IOException(E_NOT_SUPPORTED);
    }

    /*
     * The parent of source and destination can be different. It is suppose to
     * work like 'move'
     */
    @Override
    public boolean rename(Path src, Path dst) throws IOException {
      ChannelSftp channel = connect();
      try {
        boolean success = rename(channel, src, dst);
        return success;
      } finally {
        disconnect(channel);
      }
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
      ChannelSftp channel = connect();
      try {
        boolean success = delete(channel, f, recursive);
        return success;
      } finally {
        disconnect(channel);
      }
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
      ChannelSftp client = connect();
      try {
        FileStatus[] stats = listStatus(client, f);
        return stats;
      } finally {
        disconnect(client);
      }
    }

    @Override
    public void setWorkingDirectory(Path newDir) {
      // we do not maintain the working directory state
    }

    @Override
    public Path getWorkingDirectory() {
      // Return home directory always since we do not maintain state.
      return getHomeDirectory();
    }

    @Override
    public Path getHomeDirectory() {
      ChannelSftp channel = null;
      try {
        channel = connect();
        Path homeDir = new Path(channel.pwd());
        return homeDir;
      } catch (Exception ioe) {
        return null;
      } finally {
        try {
          disconnect(channel);
        } catch (IOException ioe) {
          return null;
        }
      }
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
      ChannelSftp client = connect();
      try {
        boolean success = mkdirs(client, f, permission);
        return success;
      } finally {
        disconnect(client);
      }
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      ChannelSftp channel = connect();
      try {
        FileStatus status = getFileStatus(channel, f);
        return status;
      } finally {
        disconnect(channel);
      }
    }
  }

  /** Concurrent/Multiple Connections. */
  static class SFTPConnectionPool {

    public static final Logger LOG = LoggerFactory.getLogger(FTPBatchSource.SFTPFileSystem.class);
    // Maximum number of allowed live connections. This doesn't mean we cannot
    // have more live connections. It means that when we have more
    // live connections than this threshold, any unused connection will be
    // closed.
    private int maxConnection;
    private int liveConnectionCount;
    private HashMap<ConnectionInfo, HashSet<ChannelSftp>> idleConnections =
      new HashMap<ConnectionInfo, HashSet<ChannelSftp>>();
    private HashMap<ChannelSftp, ConnectionInfo> con2infoMap =
      new HashMap<ChannelSftp, ConnectionInfo>();

    SFTPConnectionPool(int maxConnection) {
      this.maxConnection = maxConnection;
    }

    synchronized ChannelSftp getFromPool(ConnectionInfo info) throws IOException {
      Set<ChannelSftp> cons = idleConnections.get(info);
      ChannelSftp channel;

      if (cons != null && cons.size() > 0) {
        Iterator<ChannelSftp> it = cons.iterator();
        if (it.hasNext()) {
          channel = it.next();
          idleConnections.remove(info);
          return channel;
        } else {
          throw new IOException("Connection pool error.");
        }
      }
      return null;
    }

    /** Add the channel into pool.
     * @param channel
     */
    synchronized void returnToPool(ChannelSftp channel) {
      ConnectionInfo info = con2infoMap.get(channel);
      HashSet<ChannelSftp> cons = idleConnections.get(info);
      if (cons == null) {
        cons = new HashSet<ChannelSftp>();
        idleConnections.put(info, cons);
      }
      cons.add(channel);

    }

    /** Shutdown the connection pool and close all open connections. */
    synchronized void shutdown() {
      if (this.con2infoMap == null) {
        return; // already shutdown in case it is called
      }
      LOG.info("Inside shutdown, con2infoMap size=" + con2infoMap.size());

      this.maxConnection = 0;
      Set<ChannelSftp> cons = con2infoMap.keySet();
      if (cons != null && cons.size() > 0) {
        // make a copy since we need to modify the underlying Map
        Set<ChannelSftp> copy = new HashSet<ChannelSftp>(cons);
        // Initiate disconnect from all outstanding connections
        for (ChannelSftp con : copy) {
          try {
            disconnect(con);
          } catch (IOException ioe) {
            ConnectionInfo info = con2infoMap.get(con);
            LOG.error(
              "Error encountered while closing connection to " + info.getHost(),
              ioe);
          }
        }
      }
      // make sure no further connections can be returned.
      this.idleConnections = null;
      this.con2infoMap = null;
    }

    public synchronized int getMaxConnection() {
      return maxConnection;
    }

    public synchronized void setMaxConnection(int maxConn) {
      this.maxConnection = maxConn;
    }

    public ChannelSftp connect(String host, int port, String user,
                               String password, String keyFile) throws IOException {
      // get connection from pool
      ConnectionInfo info = new ConnectionInfo(host, port, user);
      ChannelSftp channel = getFromPool(info);

      if (channel != null) {
        if (channel.isConnected()) {
          return channel;
        } else {
          channel = null;
          synchronized (this) {
            --liveConnectionCount;
            con2infoMap.remove(channel);
          }
        }
      }

      // create a new connection and add to pool
      JSch jsch = new JSch();
      Session session = null;
      try {
        if (user == null || user.length() == 0) {
          user = System.getProperty("user.name");
        }

        if (password == null) {
          password = "";
        }

        if (keyFile != null && keyFile.length() > 0) {
          jsch.addIdentity(keyFile);
        }

        if (port <= 0) {
          session = jsch.getSession(user, host);
        } else {
          session = jsch.getSession(user, host, port);
        }

        session.setPassword(password);

        java.util.Properties config = new java.util.Properties();
        config.put("StrictHostKeyChecking", "no");
        session.setConfig(config);

        session.connect();
        channel = (ChannelSftp) session.openChannel("sftp");
        channel.connect();

        synchronized (this) {
          con2infoMap.put(channel, info);
          liveConnectionCount++;
        }

        return channel;

      } catch (JSchException e) {
        throw new IOException(StringUtils.stringifyException(e));
      }
    }

    void disconnect(ChannelSftp channel) throws IOException {
      if (channel != null) {
        // close connection if too many active connections
        boolean closeConnection = false;
        synchronized (this) {
          if (liveConnectionCount > maxConnection) {
            --liveConnectionCount;
            con2infoMap.remove(channel);
            closeConnection = true;
          }
        }
        if (closeConnection) {
          if (channel.isConnected()) {
            try {
              Session session = channel.getSession();
              channel.disconnect();
              session.disconnect();
            } catch (JSchException e) {
              throw new IOException(StringUtils.stringifyException(e));
            }
          }

        } else {
          returnToPool(channel);
        }
      }
    }

    public int getIdleCount() {
      return this.idleConnections.size();
    }

    public int getLiveConnCount() {
      return this.liveConnectionCount;
    }

    public int getConnPoolSize() {
      return this.con2infoMap.size();
    }

    /**
     * Class to capture the minimal set of information that distinguish
     * between different connections.
     */
    static class ConnectionInfo {
      private String host = "";
      private int port;
      private String user = "";

      ConnectionInfo(String hst, int prt, String usr) {
        this.host = hst;
        this.port = prt;
        this.user = usr;
      }

      public String getHost() {
        return host;
      }

      public void setHost(String hst) {
        this.host = hst;
      }

      public int getPort() {
        return port;
      }

      public void setPort(int prt) {
        this.port = prt;
      }

      public String getUser() {
        return user;
      }

      public void setUser(String usr) {
        this.user = usr;
      }

      @Override
      public boolean equals(Object obj) {
        if (this == obj) {
          return true;
        }

        if (obj instanceof ConnectionInfo) {
          ConnectionInfo con = (ConnectionInfo) obj;

          boolean ret = true;
          if (this.host == null || !this.host.equalsIgnoreCase(con.host)) {
            ret = false;
          }
          if (this.port >= 0 && this.port != con.port) {
            ret = false;
          }
          if (this.user == null || !this.user.equalsIgnoreCase(con.user)) {
            ret = false;
          }
          return ret;
        } else {
          return false;
        }

      }

      @Override
      public int hashCode() {
        int hashCode = 0;
        if (host != null) {
          hashCode += host.hashCode();
        }
        hashCode += port;
        if (user != null) {
          hashCode += user.hashCode();
        }
        return hashCode;
      }

    }
  }

  /**
   * {@link SFTPInputStream}, copied from Hadoop and modified, that doesn't throw an exception when seeks are attempted
   * to the current position. Position equality check logic in {@link SFTPInputStream#seek} is the only change from the
   * original class in Hadoop. This change is required since {@link LineRecordReader} calls {@link SFTPInputStream#seek}
   * with value of 0. TODO: This file can be removed once https://cdap.atlassian.net/browse/CDAP-5387 is addressed.
   */
  static class SFTPInputStream extends FSInputStream {

    public static final String E_SEEK_NOTSUPPORTED = "Seek not supported";
    public static final String E_CLIENT_NULL =
      "SFTP client null or not connected";
    public static final String E_NULL_INPUTSTREAM = "Null InputStream";
    public static final String E_STREAM_CLOSED = "Stream closed";
    public static final String E_CLIENT_NOTCONNECTED = "Client not connected";

    private InputStream wrappedStream;
    private ChannelSftp channel;
    private FileSystem.Statistics stats;
    private boolean closed;
    private long pos;

    SFTPInputStream(InputStream stream, ChannelSftp channel,
                    FileSystem.Statistics stats) {

      if (stream == null) {
        throw new IllegalArgumentException(E_NULL_INPUTSTREAM);
      }
      if (channel == null || !channel.isConnected()) {
        throw new IllegalArgumentException(E_CLIENT_NULL);
      }
      this.wrappedStream = stream;
      this.channel = channel;
      this.stats = stats;

      this.pos = 0;
      this.closed = false;
    }

    // We don't support seek unless the current position is same as the desired position.
    @Override
    public void seek(long position) throws IOException {
      // If seek is to the current pos, then simply return. This logic was added so that the seek call in
      // LineRecordReader#initialize method to '0' does not fail.
      if (getPos() == position) {
        return;
      }
      throw new IOException(E_SEEK_NOTSUPPORTED);
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      throw new IOException(E_SEEK_NOTSUPPORTED);
    }

    @Override
    public long getPos() throws IOException {
      return pos;
    }

    @Override
    public synchronized int read() throws IOException {
      if (closed) {
        throw new IOException(E_STREAM_CLOSED);
      }

      int byteRead = wrappedStream.read();
      if (byteRead >= 0) {
        pos++;
      }
      if (stats != null & byteRead >= 0) {
        stats.incrementBytesRead(1);
      }
      return byteRead;
    }

    @Override
    public synchronized int read(byte[] buf, int off, int len)
      throws IOException {
      if (closed) {
        throw new IOException(E_STREAM_CLOSED);
      }

      int result = wrappedStream.read(buf, off, len);
      if (result > 0) {
        pos += result;
      }
      if (stats != null & result > 0) {
        stats.incrementBytesRead(result);
      }

      return result;
    }

    @Override
    public synchronized void close() throws IOException {
      if (closed) {
        return;
      }
      super.close();
      closed = true;
      if (!channel.isConnected()) {
        throw new IOException(E_CLIENT_NOTCONNECTED);
      }

      try {
        Session session = channel.getSession();
        channel.disconnect();
        session.disconnect();
      } catch (JSchException e) {
        throw new IOException(StringUtils.stringifyException(e));
      }
    }
  }
}
