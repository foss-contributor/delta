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

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Integration test for reading a MANAGED_SHALLOW_CLONE table through Unity Catalog.
 *
 * <p>A shallow clone is registered in UC via the Delta REST API ({@code createTable} with {@code
 * base-table-id}); its Delta log lives under its own staging-allocated location while the AddFiles
 * reference the base table's data files by absolute path. See {@link UCShallowCloneTestBase} for
 * how the clone is constructed.
 */
public class UCDeltaTableShallowCloneReadTest extends UCShallowCloneTestBase {

  @Test
  public void testReadShallowClone() throws Exception {
    String baseTable = fullTableName("clone_read_base");
    sql(
        "CREATE TABLE %s (id INT, name STRING) USING DELTA"
            + " TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
        baseTable);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", baseTable);

    String cloneTable = fullTableName("clone_read_clone");
    createShallowClone(baseTable, "clone_read_clone");

    // All this suite asserts: the shallow clone is readable and serves the base's data.
    check(cloneTable, List.of(row("1", "a"), row("2", "b"), row("3", "c")));
  }
}
