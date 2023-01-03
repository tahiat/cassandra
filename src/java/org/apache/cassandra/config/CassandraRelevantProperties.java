/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.config;

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.virtual.LogMessagesTable;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.FileSystemOwnershipCheck;

/** A class that extracts system properties for the cassandra node it runs within. */
public enum CassandraRelevantProperties
{
    //base JVM properties
    JAVA_HOME("java.home"),
    CASSANDRA_PID_FILE ("cassandra-pidfile"),

    /**
     * Indicates the temporary directory used by the Java Virtual Machine (JVM)
     * to create and store temporary files.
     */
    JAVA_IO_TMPDIR ("java.io.tmpdir"),

    /**
     * Path from which to load native libraries.
     * Default is absolute path to lib directory.
     */
    JAVA_LIBRARY_PATH ("java.library.path"),

    JAVA_SECURITY_EGD ("java.security.egd"),

    /** Java Runtime Environment version */
    JAVA_VERSION ("java.version"),

    /** Java Virtual Machine implementation name */
    JAVA_VM_NAME ("java.vm.name"),

    /** Line separator ("\n" on UNIX). */
    LINE_SEPARATOR ("line.separator"),

    /** Java class path. */
    JAVA_CLASS_PATH ("java.class.path"),

    /** Operating system architecture. */
    OS_ARCH ("os.arch"),

    /** Operating system name. */
    OS_NAME ("os.name"),

    /** User's home directory. */
    USER_HOME ("user.home"),

    /** Platform word size sun.arch.data.model. Examples: "32", "64", "unknown"*/
    SUN_ARCH_DATA_MODEL ("sun.arch.data.model"),

    //JMX properties
    /**
     * The value of this property represents the host name string
     * that should be associated with remote stubs for locally created remote objects,
     * in order to allow clients to invoke methods on the remote object.
     */
    JAVA_RMI_SERVER_HOSTNAME ("java.rmi.server.hostname"),

    /**
     * If this value is true, object identifiers for remote objects exported by this VM will be generated by using
     * a cryptographically secure random number generator. The default value is false.
     */
    JAVA_RMI_SERVER_RANDOM_ID ("java.rmi.server.randomIDs"),

    /**
     * This property indicates whether password authentication for remote monitoring is
     * enabled. By default it is disabled - com.sun.management.jmxremote.authenticate
     */
    COM_SUN_MANAGEMENT_JMXREMOTE_AUTHENTICATE ("com.sun.management.jmxremote.authenticate"),

    /**
     * The port number to which the RMI connector will be bound - com.sun.management.jmxremote.rmi.port.
     * An Integer object that represents the value of the second argument is returned
     * if there is no port specified, if the port does not have the correct numeric format,
     * or if the specified name is empty or null.
     */
    COM_SUN_MANAGEMENT_JMXREMOTE_RMI_PORT ("com.sun.management.jmxremote.rmi.port", "0"),

    /** Cassandra jmx remote and local port */
    CASSANDRA_JMX_REMOTE_PORT("cassandra.jmx.remote.port"),
    CASSANDRA_JMX_LOCAL_PORT("cassandra.jmx.local.port"),

    /** This property  indicates whether SSL is enabled for monitoring remotely. Default is set to false. */
    COM_SUN_MANAGEMENT_JMXREMOTE_SSL ("com.sun.management.jmxremote.ssl"),

    /**
     * This property indicates whether SSL client authentication is enabled - com.sun.management.jmxremote.ssl.need.client.auth.
     * Default is set to false.
     */
    COM_SUN_MANAGEMENT_JMXREMOTE_SSL_NEED_CLIENT_AUTH ("com.sun.management.jmxremote.ssl.need.client.auth"),

    /**
     * This property indicates the location for the access file. If com.sun.management.jmxremote.authenticate is false,
     * then this property and the password and access files, are ignored. Otherwise, the access file must exist and
     * be in the valid format. If the access file is empty or nonexistent, then no access is allowed.
     */
    COM_SUN_MANAGEMENT_JMXREMOTE_ACCESS_FILE ("com.sun.management.jmxremote.access.file"),

    /** This property indicates the path to the password file - com.sun.management.jmxremote.password.file */
    COM_SUN_MANAGEMENT_JMXREMOTE_PASSWORD_FILE ("com.sun.management.jmxremote.password.file"),

    /** Port number to enable JMX RMI connections - com.sun.management.jmxremote.port */
    COM_SUN_MANAGEMENT_JMXREMOTE_PORT ("com.sun.management.jmxremote.port"),

    /**
     * A comma-delimited list of SSL/TLS protocol versions to enable.
     * Used in conjunction with com.sun.management.jmxremote.ssl - com.sun.management.jmxremote.ssl.enabled.protocols
     */
    COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_PROTOCOLS ("com.sun.management.jmxremote.ssl.enabled.protocols"),

    /**
     * A comma-delimited list of SSL/TLS cipher suites to enable.
     * Used in conjunction with com.sun.management.jmxremote.ssl - com.sun.management.jmxremote.ssl.enabled.cipher.suites
     */
    COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_CIPHER_SUITES ("com.sun.management.jmxremote.ssl.enabled.cipher.suites"),

    /** mx4jaddress */
    MX4JADDRESS ("mx4jaddress"),

    /** mx4jport */
    MX4JPORT ("mx4jport"),

    RING_DELAY("cassandra.ring_delay_ms"),

    /**
     * When bootstraping we wait for all schema versions found in gossip to be seen, and if not seen in time we fail
     * the bootstrap; this property will avoid failing and allow bootstrap to continue if set to true.
     */
    BOOTSTRAP_SKIP_SCHEMA_CHECK("cassandra.skip_schema_check"),

    /**
     * When bootstraping how long to wait for schema versions to be seen.
     */
    BOOTSTRAP_SCHEMA_DELAY_MS("cassandra.schema_delay_ms"),

    /**
     * Whether we reset any found data from previously run bootstraps.
     */
    RESET_BOOTSTRAP_PROGRESS("cassandra.reset_bootstrap_progress"),

    /**
     * When draining, how long to wait for mutating executors to shutdown.
     */
    DRAIN_EXECUTOR_TIMEOUT_MS("cassandra.drain_executor_timeout_ms", String.valueOf(TimeUnit.MINUTES.toMillis(5))),

    /**
     * Gossip quarantine delay is used while evaluating membership changes and should only be changed with extreme care.
     */
    GOSSIPER_QUARANTINE_DELAY("cassandra.gossip_quarantine_delay_ms"),

    GOSSIPER_SKIP_WAITING_TO_SETTLE("cassandra.skip_wait_for_gossip_to_settle", "-1"),

    IGNORED_SCHEMA_CHECK_VERSIONS("cassandra.skip_schema_check_for_versions"),

    IGNORED_SCHEMA_CHECK_ENDPOINTS("cassandra.skip_schema_check_for_endpoints"),

    SHUTDOWN_ANNOUNCE_DELAY_IN_MS("cassandra.shutdown_announce_in_ms", "2000"),

    /**
     * When doing a host replacement its possible that the gossip state is "empty" meaning that the endpoint is known
     * but the current state isn't known.  If the host replacement is needed to repair this state, this property must
     * be true.
     */
    REPLACEMENT_ALLOW_EMPTY("cassandra.allow_empty_replace_address", "true"),

    /**
     * Directory where Cassandra puts its logs, defaults to "." which is current directory.
     */
    LOG_DIR("cassandra.logdir", "."),

    /**
     * Directory where Cassandra persists logs from audit logging. If this property is not set, the audit log framework
     * will set it automatically to {@link CassandraRelevantProperties#LOG_DIR} + "/audit".
     */
    LOG_DIR_AUDIT("cassandra.logdir.audit"),

    CONSISTENT_DIRECTORY_LISTINGS("cassandra.consistent_directory_listings", "false"),
    CLOCK_GLOBAL("cassandra.clock", null),
    CLOCK_MONOTONIC_APPROX("cassandra.monotonic_clock.approx", null),
    CLOCK_MONOTONIC_PRECISE("cassandra.monotonic_clock.precise", null),

    /*
     * Whether {@link org.apache.cassandra.db.ConsistencyLevel#NODE_LOCAL} should be allowed.
     */
    ENABLE_NODELOCAL_QUERIES("cassandra.enable_nodelocal_queries", "false"),

    //cassandra properties (without the "cassandra." prefix)

    /**
     * The cassandra-foreground option will tell CassandraDaemon whether
     * to close stdout/stderr, but it's up to us not to background.
     * yes/null
     */
    CASSANDRA_FOREGROUND ("cassandra-foreground"),

    DEFAULT_PROVIDE_OVERLAPPING_TOMBSTONES ("default.provide.overlapping.tombstones"),
    ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION ("org.apache.cassandra.disable_mbean_registration"),

    /** This property indicates whether disable_mbean_registration is true */
    IS_DISABLED_MBEAN_REGISTRATION("org.apache.cassandra.disable_mbean_registration"),

    /** snapshots ttl cleanup period in seconds */
    SNAPSHOT_CLEANUP_PERIOD_SECONDS("cassandra.snapshot.ttl_cleanup_period_seconds", "60"),

    /** snapshots ttl cleanup initial delay in seconds */
    SNAPSHOT_CLEANUP_INITIAL_DELAY_SECONDS("cassandra.snapshot.ttl_cleanup_initial_delay_seconds", "5"),

    /** minimum allowed TTL for snapshots */
    SNAPSHOT_MIN_ALLOWED_TTL_SECONDS("cassandra.snapshot.min_allowed_ttl_seconds", "60"),

    /** what class to use for mbean registeration */
    MBEAN_REGISTRATION_CLASS("org.apache.cassandra.mbean_registration_class"),

    BATCH_COMMIT_LOG_SYNC_INTERVAL("cassandra.batch_commitlog_sync_interval_millis", "1000"),

    SYSTEM_AUTH_DEFAULT_RF("cassandra.system_auth.default_rf", "1"),
    SYSTEM_TRACES_DEFAULT_RF("cassandra.system_traces.default_rf", "2"),
    SYSTEM_DISTRIBUTED_DEFAULT_RF("cassandra.system_distributed.default_rf", "3"),

    /** Represents the maximum size (in bytes) of a serialized mutation that can be cached **/
    CACHEABLE_MUTATION_SIZE_LIMIT("cassandra.cacheable_mutation_size_limit_bytes", Long.toString(1_000_000)),

    MEMTABLE_OVERHEAD_SIZE("cassandra.memtable.row_overhead_size", "-1"),
    MEMTABLE_OVERHEAD_COMPUTE_STEPS("cassandra.memtable_row_overhead_computation_step", "100000"),
    MEMTABLE_TRIE_SIZE_LIMIT("cassandra.trie_size_limit_mb"),
    MIGRATION_DELAY("cassandra.migration_delay_ms", "60000"),
    /** Defines how often schema definitions are pulled from the other nodes */
    SCHEMA_PULL_INTERVAL_MS("cassandra.schema_pull_interval_ms", "60000"),

    PAXOS_REPAIR_RETRY_TIMEOUT_IN_MS("cassandra.paxos_repair_retry_timeout_millis", "60000"),

    /** If we should allow having duplicate keys in the config file, default to true for legacy reasons */
    ALLOW_DUPLICATE_CONFIG_KEYS("cassandra.allow_duplicate_config_keys", "true"),
    /** If we should allow having both new (post CASSANDRA-15234) and old config keys for the same config item in the yaml */
    ALLOW_NEW_OLD_CONFIG_KEYS("cassandra.allow_new_old_config_keys", "false"),

    // startup checks properties
    LIBJEMALLOC("cassandra.libjemalloc"),
    @Deprecated // should be removed in favor of enable flag of relevant startup check (checkDatacenter)
    IGNORE_DC("cassandra.ignore_dc"),
    @Deprecated // should be removed in favor of enable flag of relevant startup check (checkRack)
    IGNORE_RACK("cassandra.ignore_rack"),
    @Deprecated // should be removed in favor of enable flag of relevant startup check (FileSystemOwnershipCheck)
    FILE_SYSTEM_CHECK_ENABLE("cassandra.enable_fs_ownership_check"),
    @Deprecated // should be removed in favor of flags in relevant startup check (FileSystemOwnershipCheck)
    FILE_SYSTEM_CHECK_OWNERSHIP_FILENAME("cassandra.fs_ownership_filename", FileSystemOwnershipCheck.DEFAULT_FS_OWNERSHIP_FILENAME),
    @Deprecated // should be removed in favor of flags in relevant startup check (FileSystemOwnershipCheck)
    FILE_SYSTEM_CHECK_OWNERSHIP_TOKEN(FileSystemOwnershipCheck.FILE_SYSTEM_CHECK_OWNERSHIP_TOKEN),
    // default heartbeating period is 1 minute
    CHECK_DATA_RESURRECTION_HEARTBEAT_PERIOD("check_data_resurrection_heartbeat_period_milli", "60000"),

    // defaults to false for 4.1 but plan to switch to true in a later release
    // the thinking is that environments may not work right off the bat so safer to add this feature disabled by default
    CONFIG_ALLOW_SYSTEM_PROPERTIES("cassandra.config.allow_system_properties", "false"),

    // properties for debugging simulator ASM output
    TEST_SIMULATOR_PRINT_ASM("cassandra.test.simulator.print_asm", "none"),
    TEST_SIMULATOR_PRINT_ASM_TYPES("cassandra.test.simulator.print_asm_types", ""),
    TEST_SIMULATOR_LIVENESS_CHECK("cassandra.test.simulator.livenesscheck", "true"),
    TEST_SIMULATOR_DEBUG("cassandra.test.simulator.debug", "false"),
    TEST_SIMULATOR_DETERMINISM_CHECK("cassandra.test.simulator.determinismcheck", "none"),
    TEST_JVM_DTEST_DISABLE_SSL("cassandra.test.disable_ssl", "false"),

    // determinism properties for testing
    DETERMINISM_SSTABLE_COMPRESSION_DEFAULT("cassandra.sstable_compression_default", "true"),
    DETERMINISM_CONSISTENT_DIRECTORY_LISTINGS("cassandra.consistent_directory_listings", "false"),
    DETERMINISM_UNSAFE_UUID_NODE("cassandra.unsafe.deterministicuuidnode", "false"),
    FAILURE_LOGGING_INTERVAL_SECONDS("cassandra.request_failure_log_interval_seconds", "60"),

    // properties to disable certain behaviours for testing
    DISABLE_GOSSIP_ENDPOINT_REMOVAL("cassandra.gossip.disable_endpoint_removal"),
    IGNORE_MISSING_NATIVE_FILE_HINTS("cassandra.require_native_file_hints", "false"),
    DISABLE_SSTABLE_ACTIVITY_TRACKING("cassandra.sstable_activity_tracking", "true"),
    TEST_IGNORE_SIGAR("cassandra.test.ignore_sigar", "false"),
    PAXOS_EXECUTE_ON_SELF("cassandra.paxos.use_self_execution", "true"),

    /** property for the rate of the scheduled task that monitors disk usage */
    DISK_USAGE_MONITOR_INTERVAL_MS("cassandra.disk_usage.monitor_interval_ms", Long.toString(TimeUnit.SECONDS.toMillis(30))),

    /** property for the interval on which the repeated client warnings and diagnostic events about disk usage are ignored */
    DISK_USAGE_NOTIFY_INTERVAL_MS("cassandra.disk_usage.notify_interval_ms", Long.toString(TimeUnit.MINUTES.toMillis(30))),

    /** Controls the type of bufffer (heap/direct) used for shared scratch buffers */
    DATA_OUTPUT_BUFFER_ALLOCATE_TYPE("cassandra.dob.allocate_type"),

    // for specific tests
    ORG_APACHE_CASSANDRA_CONF_CASSANDRA_RELEVANT_PROPERTIES_TEST("org.apache.cassandra.conf.CassandraRelevantPropertiesTest"),
    ORG_APACHE_CASSANDRA_DB_VIRTUAL_SYSTEM_PROPERTIES_TABLE_TEST("org.apache.cassandra.db.virtual.SystemPropertiesTableTest"),

    // Loosen the definition of "empty" for gossip state, for use during host replacements if things go awry
    LOOSE_DEF_OF_EMPTY_ENABLED(Config.PROPERTY_PREFIX + "gossiper.loose_empty_enabled"),

    // Maximum number of rows in system_views.logs table
    LOGS_VIRTUAL_TABLE_MAX_ROWS("cassandra.virtual.logs.max.rows", Integer.toString(LogMessagesTable.LOGS_VIRTUAL_TABLE_DEFAULT_ROWS)),

    /** Used when running in Client mode and the system and schema keyspaces need to be initialized outside of their normal initialization path **/
    FORCE_LOAD_LOCAL_KEYSPACES("cassandra.schema.force_load_local_keyspaces"),

    // commit log relevant properties
    /**
     * Entities to replay mutations for upon commit log replay, property is meant to contain
     * comma-separated entities which are either names of keyspaces or keyspaces and tables or their mix.
     * Examples:
     * just keyspaces
     * -Dcassandra.replayList=ks1,ks2,ks3
     * specific tables
     * -Dcassandra.replayList=ks1.tb1,ks2.tb2
     * mix of tables and keyspaces
     * -Dcassandra.replayList=ks1.tb1,ks2
     *
     * If only keyspaces are specified, mutations for all tables in such keyspace will be replayed
     * */
    COMMIT_LOG_REPLAY_LIST("cassandra.replayList", null),

    SSTABLE_FORMAT_DEFAULT("cassandra.sstable.format.default", "BIG")

    ;

    CassandraRelevantProperties(String key, String defaultVal)
    {
        this.key = key;
        this.defaultVal = defaultVal;
    }

    CassandraRelevantProperties(String key)
    {
        this.key = key;
        this.defaultVal = null;
    }

    private final String key;
    private final String defaultVal;

    public String getKey()
    {
        return key;
    }

    /**
     * Gets the value of the indicated system property.
     * @return system property value if it exists, defaultValue otherwise.
     */
    public String getString()
    {
        String value = System.getProperty(key);

        return value == null ? defaultVal : STRING_CONVERTER.convert(value);
    }

    /**
     * Returns default value.
     *
     * @return default value, if any, otherwise null.
     */
    public String getDefaultValue()
    {
        return defaultVal;
    }

    /**
     * Gets the value of a system property as a String.
     * @return system property String value if it exists, overrideDefaultValue otherwise.
     */
    public String getString(String overrideDefaultValue)
    {
        String value = System.getProperty(key);
        if (value == null)
            return overrideDefaultValue;

        return STRING_CONVERTER.convert(value);
    }

    public <T> T convert(PropertyConverter<T> converter)
    {
        String value = System.getProperty(key);
        if (value == null)
            value = defaultVal;

        return converter.convert(value);
    }

    /**
     * Sets the value into system properties.
     * @param value to set
     */
    public void setString(String value)
    {
        System.setProperty(key, value);
    }

    /**
     * Gets the value of a system property as a boolean.
     * @return system property boolean value if it exists, false otherwise().
     */
    public boolean getBoolean()
    {
        String value = System.getProperty(key);

        return BOOLEAN_CONVERTER.convert(value == null ? defaultVal : value);
    }

    /**
     * Gets the value of a system property as a boolean.
     * @return system property boolean value if it exists, overrideDefaultValue otherwise.
     */
    public boolean getBoolean(boolean overrideDefaultValue)
    {
        String value = System.getProperty(key);
        if (value == null)
            return overrideDefaultValue;

        return BOOLEAN_CONVERTER.convert(value);
    }

    /**
     * Sets the value into system properties.
     * @param value to set
     */
    public void setBoolean(boolean value)
    {
        System.setProperty(key, Boolean.toString(value));
    }

    /**
     * Clears the value set in the system property.
     */
    public void clearValue()
    {
        System.clearProperty(key);
    }

    /**
     * Gets the value of a system property as a int.
     * @return system property int value if it exists, defaultValue otherwise.
     */
    public int getInt()
    {
        String value = System.getProperty(key);

        return INTEGER_CONVERTER.convert(value == null ? defaultVal : value);
    }

    /**
     * Gets the value of a system property as a int.
     * @return system property int value if it exists, defaultValue otherwise.
     */
    public long getLong()
    {
        String value = System.getProperty(key);

        return LONG_CONVERTER.convert(value == null ? defaultVal : value);
    }

    /**
     * Gets the value of a system property as a int.
     * @return system property int value if it exists, overrideDefaultValue otherwise.
     */
    public int getInt(int overrideDefaultValue)
    {
        String value = System.getProperty(key);
        if (value == null)
            return overrideDefaultValue;

        return INTEGER_CONVERTER.convert(value);
    }

    /**
     * Sets the value into system properties.
     * @param value to set
     */
    public void setInt(int value)
    {
        System.setProperty(key, Integer.toString(value));
    }

    /**
     * Sets the value into system properties.
     * @param value to set
     */
    public void setLong(long value)
    {
        System.setProperty(key, Long.toString(value));
    }

    /**
     * Gets the value of a system property as a enum, calling {@link String#toUpperCase()} first.
     *
     * @param defaultValue to return when not defined
     * @param <T> type
     * @return enum value
     */
    public <T extends Enum<T>> T getEnum(T defaultValue) {
        return getEnum(true, defaultValue);
    }

    /**
     * Gets the value of a system property as a enum, optionally calling {@link String#toUpperCase()} first.
     *
     * @param toUppercase before converting to enum
     * @param defaultValue to return when not defined
     * @param <T> type
     * @return enum value
     */
    public <T extends Enum<T>> T getEnum(boolean toUppercase, T defaultValue) {
        String value = System.getProperty(key);
        if (value == null)
            return defaultValue;
        return Enum.valueOf(defaultValue.getDeclaringClass(), toUppercase ? value.toUpperCase() : value);
    }

    /**
     * Gets the value of a system property as an enum, optionally calling {@link String#toUpperCase()} first.
     * If the value is missing, the default value for this property is used
     *
     * @param toUppercase before converting to enum
     * @param enumClass enumeration class
     * @param <T> type
     * @return enum value
     */
    public <T extends Enum<T>> T getEnum(boolean toUppercase, Class<T> enumClass)
    {
        String value = System.getProperty(key, defaultVal);
        return Enum.valueOf(enumClass, toUppercase ? value.toUpperCase() : value);
    }

    /**
     * Sets the value into system properties.
     * @param value to set
     */
    public void setEnum(Enum<?> value) {
        System.setProperty(key, value.name());
    }

    public interface PropertyConverter<T>
    {
        T convert(String value);
    }

    private static final PropertyConverter<String> STRING_CONVERTER = value -> value;

    private static final PropertyConverter<Boolean> BOOLEAN_CONVERTER = Boolean::parseBoolean;

    private static final PropertyConverter<Integer> INTEGER_CONVERTER = value ->
    {
        try
        {
            return Integer.decode(value);
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("Invalid value for system property: " +
                                                           "expected integer value but got '%s'", value));
        }
    };

    private static final PropertyConverter<Long> LONG_CONVERTER = value ->
    {
        try
        {
            return Long.decode(value);
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("Invalid value for system property: " +
                                                           "expected integer value but got '%s'", value));
        }
    };

    /**
     * @return whether a system property is present or not.
     */
    public boolean isPresent()
    {
        return System.getProperties().containsKey(key);
    }
}