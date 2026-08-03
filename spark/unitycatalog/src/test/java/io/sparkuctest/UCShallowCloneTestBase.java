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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.unitycatalog.client.delta.api.DeltaTablesApi;
import io.unitycatalog.client.delta.model.DeltaCreateStagingTableRequest;
import io.unitycatalog.client.delta.model.DeltaCreateTableRequest;
import io.unitycatalog.client.delta.model.DeltaLoadTableResponse;
import io.unitycatalog.client.delta.model.DeltaProtocol;
import io.unitycatalog.client.delta.model.DeltaStagingTableResponse;
import io.unitycatalog.client.delta.model.DeltaTableType;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Shared scaffolding for shallow-clone integration tests.
 *
 * <p>Spark has no {@code CREATE TABLE ... SHALLOW CLONE} support for UC catalog-managed tables yet,
 * so {@link #createShallowClone} plays the engine's role: it stages the clone, writes the clone's
 * commit 0 (the base's protocol/metadata plus absolute-path AddFiles), and finalizes the create
 * through the UC Delta REST API with {@code table-type=MANAGED_SHALLOW_CLONE} and {@code
 * base-table-id}.
 */
public abstract class UCShallowCloneTestBase extends UCDeltaTableIntegrationBaseTest {

  protected static final ObjectMapper MAPPER = new ObjectMapper();

  /** Creates the clone next to the base, in the harness's default catalog and schema. */
  protected UUID createShallowClone(String baseTable, String cloneName) throws Exception {
    UnityCatalogInfo uc = unityCatalogInfo();
    return createShallowClone(baseTable, uc.catalogName(), uc.schemaName(), cloneName);
  }

  /**
   * Registers {@code cloneCatalog.cloneSchema.cloneName} as a MANAGED_SHALLOW_CLONE of {@code
   * baseTable} (a three-part name) via the Delta REST API, doing the engine's share of the work:
   * stage the clone, write its commit 0 (the base's protocol and metadata with the clone's table
   * id, plus AddFiles referencing the base's live data files by absolute path), then finalize with
   * {@code base-table-id}. Returns the base table's UUID.
   */
  protected UUID createShallowClone(
      String baseTable, String cloneCatalog, String cloneSchema, String cloneName)
      throws Exception {
    String[] baseParts = baseTable.split("\\.");
    assertThat(baseParts).as("base table name must be catalog.schema.table").hasSize(3);
    DeltaTablesApi tablesApi = new DeltaTablesApi(unityCatalogInfo().createApiClient());

    DeltaLoadTableResponse base = tablesApi.loadTable(baseParts[0], baseParts[1], baseParts[2]);
    UUID baseTableId = base.getMetadata().getTableUuid();

    DeltaStagingTableResponse staging =
        tablesApi.createStagingTable(
            cloneCatalog, cloneSchema, new DeltaCreateStagingTableRequest().name(cloneName));

    JsonNode protocolJson =
        writeCloneCommitZero(
            base.getMetadata().getLocation(),
            staging.getLocation(),
            baseTableId.toString(),
            staging.getTableId().toString(),
            baseTable);

    // The clone's catalog entry mirrors the base (same UC catalog-managed contract), with the
    // engine-generated ucTableId swapped for the clone's own staging-allocated id.
    Map<String, String> properties = new HashMap<>(base.getMetadata().getProperties());
    properties.replaceAll(
        (k, v) -> v.equals(baseTableId.toString()) ? staging.getTableId().toString() : v);

    tablesApi.createTable(
        cloneCatalog,
        cloneSchema,
        new DeltaCreateTableRequest()
            .name(cloneName)
            .location(staging.getLocation())
            .tableType(DeltaTableType.MANAGED_SHALLOW_CLONE)
            .baseTableId(baseTableId)
            .columns(base.getMetadata().getColumns())
            .partitionColumns(base.getMetadata().getPartitionColumns())
            .protocol(toProtocolModel(protocolJson))
            .properties(properties)
            .lastCommitTimestampMs(System.currentTimeMillis()));
    return baseTableId;
  }

  /**
   * Writes the clone's {@code _delta_log/00000000000000000000.json} from the base table's commit 0:
   * protocol and commitInfo are carried over verbatim, metaData gets the clone's table id,
   * domainMetadata is dropped (write-side state the clone does not inherit), and one AddFile is
   * appended per live base data file using the absolute path. Returns the protocol action's JSON
   * for reuse in the createTable request.
   */
  private JsonNode writeCloneCommitZero(
      String baseLocation,
      String cloneLocation,
      String baseTableId,
      String cloneTableId,
      String baseTable)
      throws Exception {
    Path baseCommitZero =
        localPath(baseLocation).resolve("_delta_log").resolve("00000000000000000000.json");
    assertThat(baseCommitZero)
        .withFailMessage("Base table's published commit 0 not found at %s", baseCommitZero)
        .exists();

    List<String> lines = new ArrayList<>();
    JsonNode protocolJson = null;
    for (String line : Files.readAllLines(baseCommitZero)) {
      if (line.isBlank()) continue;
      ObjectNode action = (ObjectNode) MAPPER.readTree(line);
      if (action.has("domainMetadata")) continue;
      if (action.has("protocol")) protocolJson = action.get("protocol");
      if (action.has("metaData")) {
        ObjectNode metaData = (ObjectNode) action.get("metaData");
        metaData.put("id", cloneTableId);
        ObjectNode configuration = (ObjectNode) metaData.get("configuration");
        if (configuration != null) {
          List<String> keys = new ArrayList<>();
          configuration.fieldNames().forEachRemaining(keys::add);
          for (String key : keys) {
            if (configuration.get(key).asText().equals(baseTableId)) {
              configuration.put(key, cloneTableId);
            }
          }
        }
        line = MAPPER.writeValueAsString(action);
      }
      lines.add(line);
    }
    assertThat(protocolJson)
        .withFailMessage("No protocol action in base commit 0 at %s", baseCommitZero)
        .isNotNull();

    for (List<String> file :
        sql(
            "SELECT DISTINCT _metadata.file_path, _metadata.file_size,"
                + " unix_millis(_metadata.file_modification_time) FROM %s",
            baseTable)) {
      ObjectNode add = MAPPER.createObjectNode();
      add.put("path", file.get(0));
      add.set("partitionValues", MAPPER.createObjectNode());
      add.put("size", Long.parseLong(file.get(1)));
      add.put("modificationTime", Long.parseLong(file.get(2)));
      add.put("dataChange", true);
      ObjectNode action = MAPPER.createObjectNode();
      action.set("add", add);
      lines.add(MAPPER.writeValueAsString(action));
    }

    Path cloneLog = localPath(cloneLocation).resolve("_delta_log");
    Files.createDirectories(cloneLog);
    Files.write(cloneLog.resolve("00000000000000000000.json"), lines);
    return protocolJson;
  }

  private static DeltaProtocol toProtocolModel(JsonNode protocolJson) {
    DeltaProtocol protocol =
        new DeltaProtocol()
            .minReaderVersion(protocolJson.get("minReaderVersion").asInt())
            .minWriterVersion(protocolJson.get("minWriterVersion").asInt());
    if (protocolJson.has("readerFeatures")) {
      protocol.readerFeatures(toStringList(protocolJson.get("readerFeatures")));
    }
    if (protocolJson.has("writerFeatures")) {
      protocol.writerFeatures(toStringList(protocolJson.get("writerFeatures")));
    }
    return protocol;
  }

  private static List<String> toStringList(JsonNode array) {
    List<String> values = new ArrayList<>();
    array.forEach(node -> values.add(node.asText()));
    return values;
  }

  /**
   * Maps a UC location to the backing local filesystem path: {@code file:} URIs directly (managed
   * tables live under the server's local storage root), {@code s3://<bucket>/<abs-path>} through
   * the fake-bucket mapping.
   */
  protected static Path localPath(String location) {
    if (location.startsWith("file:")) {
      return Paths.get(URI.create(location).getPath());
    }
    return Paths.get(location.replaceFirst("^s3://[^/]+", ""));
  }

  /** The table name without the catalog and schema qualifiers. */
  protected static String simpleName(String fullTableName) {
    return fullTableName.substring(fullTableName.lastIndexOf('.') + 1);
  }
}
