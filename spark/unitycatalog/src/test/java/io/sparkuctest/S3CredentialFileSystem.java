/*
 * Copyright (2026) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.sparkuctest;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

/**
 * Fake S3 filesystem backed by local disk for integration testing. Maps {@code s3://bucket/path} to
 * local paths while verifying UC-vended credentials are correctly propagated.
 */
public class S3CredentialFileSystem extends RawLocalFileSystem {

  private static final String SCHEME = "s3:";

  // Same as org.apache.hadoop.fs.s3a.Constants#AWS_CREDENTIALS_PROVIDER.
  private static final String S3A_CREDENTIALS_PROVIDER = "fs.s3a.aws.credentials.provider";

  /** Set to {@code false} to skip credential assertions (useful for debugging). */
  public static boolean credentialCheckEnabled = true;

  private AwsCredentialsProvider provider;

  @Override
  protected void checkPath(Path path) {
    // Accept any path without validation.
  }

  @Override
  public FSDataOutputStream create(
      Path f,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {
    return super.create(toLocalPath(f), overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    if (!f.toString().startsWith(SCHEME)) return super.getFileStatus(f);
    return restoreS3Path(f, super.getFileStatus(toLocalPath(f)));
  }

  @Override
  public FSDataInputStream open(Path f) throws IOException {
    return super.open(toLocalPath(f));
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    FileStatus[] files;
    try {
      files = super.listStatus(toLocalPath(f));
    } catch (FileNotFoundException e) {
      return new FileStatus[0]; // S3 returns empty for non-existent prefixes
    }
    FileStatus[] result = new FileStatus[files.length];
    for (int i = 0; i < files.length; i++) {
      result[i] = restoreS3Path(f, files[i]);
    }
    return result;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return super.mkdirs(toLocalPath(f), permission);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return super.rename(toLocalPath(src), toLocalPath(dst));
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return super.delete(toLocalPath(f), recursive);
  }

  /** Converts {@code s3://bucket/path} to local path, verifying credentials on the way. */
  private Path toLocalPath(Path f) {
    checkCredentials(f);
    return new Path(f.toString().replaceAll(SCHEME + "//.*?/", "file:///"));
  }

  /** Replaces the file: scheme in a FileStatus with the original S3 prefix. */
  private FileStatus restoreS3Path(Path originalS3Path, FileStatus status) {
    String s3Prefix = SCHEME + "//" + originalS3Path.toUri().getHost();
    String restored = status.getPath().toString().replace("file:", s3Prefix);
    return new FileStatus(
        status.getLen(),
        status.isDirectory(),
        status.getReplication(),
        status.getBlockSize(),
        status.getModificationTime(),
        new Path(restored));
  }

  /** Expected credentials per fake bucket: access key, secret key, session token. */
  private static final Map<String, List<String>> EXPECTED_BUCKET_CREDENTIALS =
      Map.of(
          UnityCatalogSupport.FAKE_S3_BUCKET,
          List.of("fakeAccessKey", "fakeSecretKey", "fakeSessionToken"),
          UnityCatalogSupport.FAKE_S3_BUCKET_B,
          List.of("fakeAccessKeyB", "fakeSecretKeyB", "fakeSessionTokenB"));

  private void checkCredentials(Path f) {
    if (!credentialCheckEnabled) return;
    String bucket = f.toUri().getHost();
    List<String> expected = EXPECTED_BUCKET_CREDENTIALS.get(bucket);
    assertThat(expected).withFailMessage("Unexpected bucket %s", bucket).isNotNull();
    assertCredentials(bucket, expected);
  }

  /**
   * Verifies the credentials applied for {@code bucket} are that bucket's own. Accepts S3A
   * per-bucket keys when present (the shape a multi-credential client should emit, and the only
   * shape that can carry two credential sets at once); otherwise falls back to the
   * AwsCredentialsProvider or the global Hadoop properties.
   */
  private void assertCredentials(String bucket, List<String> expected) {
    Configuration conf = getConf();
    String perBucketAccessKey = conf.get("fs.s3a.bucket." + bucket + ".access.key");
    if (perBucketAccessKey != null) {
      assertThat(perBucketAccessKey)
          .as("access key for bucket %s", bucket)
          .isEqualTo(expected.get(0));
      assertThat(conf.get("fs.s3a.bucket." + bucket + ".secret.key"))
          .as("secret key for bucket %s", bucket)
          .isEqualTo(expected.get(1));
      assertThat(conf.get("fs.s3a.bucket." + bucket + ".session.token"))
          .as("session token for bucket %s", bucket)
          .isEqualTo(expected.get(2));
      return;
    }
    AwsCredentialsProvider p = resolveProvider(conf);
    if (p != null) {
      AwsSessionCredentials creds = (AwsSessionCredentials) p.resolveCredentials();
      assertThat(creds.accessKeyId())
          .as("access key for bucket %s", bucket)
          .isEqualTo(expected.get(0));
      assertThat(creds.secretAccessKey())
          .as("secret key for bucket %s", bucket)
          .isEqualTo(expected.get(1));
      assertThat(creds.sessionToken())
          .as("session token for bucket %s", bucket)
          .isEqualTo(expected.get(2));
    } else {
      assertThat(conf.get("fs.s3a.access.key"))
          .as("access key for bucket %s", bucket)
          .isEqualTo(expected.get(0));
      assertThat(conf.get("fs.s3a.secret.key"))
          .as("secret key for bucket %s", bucket)
          .isEqualTo(expected.get(1));
      assertThat(conf.get("fs.s3a.session.token"))
          .as("session token for bucket %s", bucket)
          .isEqualTo(expected.get(2));
    }
  }

  private synchronized AwsCredentialsProvider resolveProvider(Configuration conf) {
    if (provider != null) return provider;
    String clazz = conf.get(S3A_CREDENTIALS_PROVIDER);
    if (clazz == null) return null;
    try {
      provider =
          (AwsCredentialsProvider)
              Class.forName(clazz).getConstructor(Configuration.class).newInstance(conf);
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate credential provider: " + clazz, e);
    }
    return provider;
  }
}
