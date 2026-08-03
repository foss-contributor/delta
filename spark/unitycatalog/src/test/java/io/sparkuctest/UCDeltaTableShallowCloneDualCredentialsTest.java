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

import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.delta.api.DeltaTablesApi;
import io.unitycatalog.client.model.CreateSchema;
import java.nio.file.Files;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Reading a shallow clone whose base table lives under a different bucket requires TWO credential
 * sets at once: the clone's own (its Delta log and location) and READ on the base's location (the
 * data files the clone's log points at). UC vends both entries from the credentials endpoint; this
 * suite pins that the read path actually applies both.
 *
 * <p>The two credential scopes are built from per-schema {@code storage_root}s placed in two fake
 * buckets that the UC server vends different credentials for; {@link S3CredentialFileSystem}
 * asserts that every path access carries that bucket's own credentials.
 */
public class UCDeltaTableShallowCloneDualCredentialsTest extends UCShallowCloneTestBase {

  private static final String SCHEMA_A = "clone_creds_a";
  private static final String SCHEMA_B = "clone_creds_b";

  @Test
  public void testReadShallowCloneWithBaseInDifferentBucket() throws Exception {
    // Multi-location credentials are applied per path by the credential-scoped FileSystem;
    // without it the base table's scope is never applied and this read cannot succeed. Must be
    // set before this session touches the catalog for the first time.
    spark()
        .conf()
        .set(
            "spark.sql.catalog." + unityCatalogInfo().catalogName() + ".credScopedFs.enabled",
            "true");
    // Per-scan credentials must actually reach the filesystem: the JVM-wide FileSystem cache
    // would otherwise serve the base bucket's FS instance created under the base scan's
    // configuration, masking missing credentials in the clone scan.
    spark().sparkContext().hadoopConfiguration().set("fs.s3.impl.disable.cache", "true");

    UnityCatalogInfo uc = unityCatalogInfo();
    SchemasApi schemasApi = new SchemasApi(uc.createApiClient());
    schemasApi.createSchema(
        new CreateSchema()
            .name(SCHEMA_A)
            .catalogName(uc.catalogName())
            .storageRoot(
                "s3://"
                    + UnityCatalogSupport.FAKE_S3_BUCKET
                    + Files.createTempDirectory("clone-bucket-a-")));
    schemasApi.createSchema(
        new CreateSchema()
            .name(SCHEMA_B)
            .catalogName(uc.catalogName())
            .storageRoot(
                "s3://"
                    + UnityCatalogSupport.FAKE_S3_BUCKET_B
                    + Files.createTempDirectory("clone-bucket-b-")));

    String baseTable = uc.catalogName() + "." + SCHEMA_A + ".cross_bucket_base";
    sql(
        "CREATE TABLE %s (id INT, name STRING) USING DELTA"
            + " TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
        baseTable);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", baseTable);
    // Pre-flight: single-scope access works -- the base reads fine with its own credentials.
    check(baseTable, List.of(row("1", "a"), row("2", "b"), row("3", "c")));

    String cloneTable = uc.catalogName() + "." + SCHEMA_B + ".cross_bucket_clone";
    createShallowClone(baseTable, uc.catalogName(), SCHEMA_B, "cross_bucket_clone");

    // Guard the premise: the two tables really live under different buckets, so reading the
    // clone needs both buckets' credentials.
    DeltaTablesApi tablesApi = new DeltaTablesApi(uc.createApiClient());
    assertThat(
            tablesApi
                .loadTable(uc.catalogName(), SCHEMA_A, "cross_bucket_base")
                .getMetadata()
                .getLocation())
        .startsWith("s3://" + UnityCatalogSupport.FAKE_S3_BUCKET + "/");
    assertThat(
            tablesApi
                .loadTable(uc.catalogName(), SCHEMA_B, "cross_bucket_clone")
                .getMetadata()
                .getLocation())
        .startsWith("s3://" + UnityCatalogSupport.FAKE_S3_BUCKET_B + "/");

    // The actual contract under test: the clone's own credentials cover its log under bucket B,
    // and the base-table READ credential entry covers the data files under bucket A.
    check(cloneTable, List.of(row("1", "a"), row("2", "b"), row("3", "c")));
  }
}
