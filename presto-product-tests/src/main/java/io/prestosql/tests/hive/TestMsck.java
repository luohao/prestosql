/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.tests.hive;

import com.google.inject.Inject;
import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.hadoop.hdfs.HdfsClient;
import io.prestodb.tempto.query.QueryResult;
import org.testng.annotations.Test;

import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.TestGroups.HIVE_PARTITIONING;
import static io.prestosql.tests.TestGroups.SMOKE;

public class TestMsck
        extends ProductTest
{
    private static final String TABLE_NAME = "test_msck";
    private static final String WAREHOUSE_DIRECTORY_PATH = "/user/hive/warehouse/";

    @Inject
    private HdfsClient hdfsClient;

    @Test(groups = {HIVE_PARTITIONING, SMOKE})
    public void testMsckAddPartition()
    {
        query("CREATE TABLE " + TABLE_NAME + " (payload bigint, x varchar, y varchar) WITH (format = 'ORC', partitioned_by = ARRAY[ 'x', 'y' ])");
        query("CALL system.create_empty_partition('default', '" + TABLE_NAME + "', ARRAY['x','y'], ARRAY['a','1'])");
        query("CALL system.create_empty_partition('default', '" + TABLE_NAME + "', ARRAY['x','y'], ARRAY['b','2'])");

        String tableLocation = WAREHOUSE_DIRECTORY_PATH + TABLE_NAME;
        hdfsClient.createDirectory(tableLocation + "/x=f/y=9");
        hdfsClient.delete(tableLocation + "/x=b/y=2");

        // add invalid partition path
        hdfsClient.createDirectory(tableLocation + "/y=3/x=h");
        hdfsClient.createDirectory(tableLocation + "/y=3");
        hdfsClient.createDirectory(tableLocation + "/xyz");

        query("CALL system.msck('default', '" + TABLE_NAME + "', true, false)");
        QueryResult partitionListResult = query("SELECT * FROM \"" + TABLE_NAME + "$partitions\"");
        assertThat(partitionListResult).containsExactly(row("a", "1"), row("b", "2"), row("f", "9"));

        query("DROP TABLE " + TABLE_NAME);
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE})
    public void testMsckDropPartition()
    {
        query("CREATE TABLE " + TABLE_NAME + " (payload bigint, x varchar, y varchar) WITH (format = 'ORC', partitioned_by = ARRAY[ 'x', 'y' ])");
        query("CALL system.create_empty_partition('default', '" + TABLE_NAME + "', ARRAY['x','y'], ARRAY['a','1'])");
        query("CALL system.create_empty_partition('default', '" + TABLE_NAME + "', ARRAY['x','y'], ARRAY['b','2'])");

        String tableLocation = WAREHOUSE_DIRECTORY_PATH + TABLE_NAME;
        hdfsClient.createDirectory(tableLocation + "/x=f/y=9");
        hdfsClient.delete(tableLocation + "/x=b/y=2");

        // add invalid partition path
        hdfsClient.createDirectory(tableLocation + "/y=3/x=h");
        hdfsClient.createDirectory(tableLocation + "/y=3");
        hdfsClient.createDirectory(tableLocation + "/xyz");

        query("CALL system.msck('default', '" + TABLE_NAME + "', false, true)");
        QueryResult partitionListResult = query("SELECT * FROM \"" + TABLE_NAME + "$partitions\"");
        assertThat(partitionListResult).containsExactly(row("a", "1"));

        query("DROP TABLE " + TABLE_NAME);
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE})
    public void testMsckAddDropPartition()
    {
        query("CREATE TABLE " + TABLE_NAME + " (payload bigint, x varchar, y varchar) WITH (format = 'ORC', partitioned_by = ARRAY[ 'x', 'y' ])");
        query("CALL system.create_empty_partition('default', '" + TABLE_NAME + "', ARRAY['x','y'], ARRAY['a','1'])");
        query("CALL system.create_empty_partition('default', '" + TABLE_NAME + "', ARRAY['x','y'], ARRAY['b','2'])");

        String tableLocation = WAREHOUSE_DIRECTORY_PATH + TABLE_NAME;
        hdfsClient.createDirectory(tableLocation + "/x=f/y=9");
        hdfsClient.delete(tableLocation + "/x=b/y=2");

        // add invalid partition path
        hdfsClient.createDirectory(tableLocation + "/y=3/x=h");
        hdfsClient.createDirectory(tableLocation + "/y=3");
        hdfsClient.createDirectory(tableLocation + "/xyz");

        query("CALL system.msck('default', '" + TABLE_NAME + "', true, true)");
        QueryResult partitionListResult = query("SELECT * FROM \"" + TABLE_NAME + "$partitions\"");
        assertThat(partitionListResult).containsExactly(row("a", "1"), row("f", "9"));

        query("DROP TABLE " + TABLE_NAME);
    }
}
