package com.dexecr.kafka.manager.model;

import java.util.HashMap;
import java.util.Map;

import javax.validation.constraints.DecimalMax;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.Min;
import javax.validation.constraints.Pattern;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;

import org.apache.kafka.common.config.TopicConfig;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import io.micronaut.core.annotation.Introspected;

import static java.util.Optional.ofNullable;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
@EqualsAndHashCode
@JsonIgnoreProperties(ignoreUnknown = true)
@Introspected
public class TopicConfiguration {
    private static final String CLEANUP_POLICY_CONFIG = TopicConfig.CLEANUP_POLICY_CONFIG;
    private static final String COMPRESSION_TYPE_CONFIG = TopicConfig.COMPRESSION_TYPE_CONFIG;
    private static final String DELETE_RETENTION_MS_CONFIG = TopicConfig.DELETE_RETENTION_MS_CONFIG;
    private static final String FILE_DELETE_DELAY_MS_CONFIG = TopicConfig.FILE_DELETE_DELAY_MS_CONFIG;
    private static final String FLUSH_MESSAGES_INTERVAL_CONFIG = TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG;
    private static final String FLUSH_MS_CONFIG = TopicConfig.FLUSH_MS_CONFIG;
    private static final String INDEX_INTERVAL_BYTES_CONFIG = TopicConfig.INDEX_INTERVAL_BYTES_CONFIG;
    private static final String MAX_MESSAGE_BYTES_CONFIG = TopicConfig.MAX_MESSAGE_BYTES_CONFIG;
    private static final String MESSAGE_FORMAT_VERSION_CONFIG = TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG;
    private static final String MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG = TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG;
    private static final String MESSAGE_TIMESTAMP_TYPE_CONFIG = TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG;
    private static final String MIN_CLEANABLE_DIRTY_RATIO_CONFIG = TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG;
    private static final String MIN_COMPACTION_LAG_MS_CONFIG = TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG;
    private static final String MIN_IN_SYNC_REPLICAS_CONFIG = TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG;
    private static final String PREALLOCATE_CONFIG = TopicConfig.PREALLOCATE_CONFIG;
    private static final String RETENTION_BYTES_CONFIG = TopicConfig.RETENTION_BYTES_CONFIG;
    private static final String RETENTION_MS_CONFIG = TopicConfig.RETENTION_MS_CONFIG;
    private static final String SEGMENT_BYTES_CONFIG = TopicConfig.SEGMENT_BYTES_CONFIG;
    private static final String SEGMENT_INDEX_BYTES_CONFIG = TopicConfig.SEGMENT_INDEX_BYTES_CONFIG;
    private static final String SEGMENT_JITTER_MS_CONFIG = TopicConfig.SEGMENT_JITTER_MS_CONFIG;
    private static final String SEGMENT_MS_CONFIG = TopicConfig.SEGMENT_MS_CONFIG;
    private static final String UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG = TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG;
    private static final String MESSAGE_DOWNCONVERSION_ENABLE_CONFIG = TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG;

    private static final String LEADER_REPLICATION_THROTTLED_REPLICAS = "leader.replication.throttled.replicas";
    private static final String FOLLOWER_REPLICATION_THROTTLED_REPLICAS = "follower.replication.throttled.replicas";

    @JsonProperty(CLEANUP_POLICY_CONFIG)
    @Pattern(regexp = "(.*compact.*)|(.*delete.*)", message = "Invalid cleanup policy!")
    String cleanupPolicy;

    @JsonProperty(COMPRESSION_TYPE_CONFIG)
    @Pattern(regexp = "uncompressed|snappy|lz4|gzip|zstd|producer", message = "Invalid compression type!")
    String compressionType;

    @JsonProperty(DELETE_RETENTION_MS_CONFIG)
    @Min(0)
    @JsonSerialize(using = ToStringSerializer.class)
    Long deleteRetentionMs;

    @JsonProperty(FILE_DELETE_DELAY_MS_CONFIG)
    @Min(0)
    @JsonSerialize(using = ToStringSerializer.class)
    Long fileDeleteDelayMs;

    @JsonProperty(FLUSH_MESSAGES_INTERVAL_CONFIG)
    @Min(0)
    @JsonSerialize(using = ToStringSerializer.class)
    Long flushMessages;

    @JsonProperty(FLUSH_MS_CONFIG)
    @Min(0)
    @JsonSerialize(using = ToStringSerializer.class)
    Long flushMs;

    @JsonProperty(FOLLOWER_REPLICATION_THROTTLED_REPLICAS)
    @Pattern(regexp = "^(((\\d+:\\d+)[,])+(\\d+:\\d+))|(\\*)|(\\d+:\\d+)$", message = "Invalid follower replication throttled replicas")
    String followerReplicationThrottledReplicas;

    @JsonProperty(INDEX_INTERVAL_BYTES_CONFIG)
    @Min(0)
    Integer indexIntervalBytes;

    @JsonProperty(LEADER_REPLICATION_THROTTLED_REPLICAS)
    @Pattern(regexp = "^(((\\d+:\\d+)[,]{1})+(\\d+:\\d+))|(\\*)|(\\d+:\\d+)$", message = "Invalid leader replication throttled replicas")
    String leaderReplicationThrottledReplicas;

    @JsonProperty(MAX_MESSAGE_BYTES_CONFIG)
    @Min(0)
    Integer maxMessageBytes;

    @JsonProperty(MESSAGE_FORMAT_VERSION_CONFIG)
    String messageFormatVersion;

    @JsonProperty(MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG)
    @Min(0)
    @JsonSerialize(using = ToStringSerializer.class)
    Long messageTimestampDifferenceMaxMs;

    @JsonProperty(MESSAGE_TIMESTAMP_TYPE_CONFIG)
    @Pattern(regexp = "CreateTime|LogAppendTime", message = "Invalid message timestamp type!")
    String messageTimestampType;

    @JsonProperty(MIN_CLEANABLE_DIRTY_RATIO_CONFIG)
    @DecimalMin("0.0")
    @DecimalMax("1.0")
    Double minCleanableDirtyRatio;

    @JsonProperty(MIN_COMPACTION_LAG_MS_CONFIG)
    @Min(0)
    @JsonSerialize(using = ToStringSerializer.class)
    Long minCompactionLagMs;

    @JsonProperty(MIN_IN_SYNC_REPLICAS_CONFIG)
    @Min(1)
    Integer minInsyncReplicas;

    @JsonProperty(PREALLOCATE_CONFIG)
    Boolean preallocate;

    @JsonProperty(RETENTION_BYTES_CONFIG)
    @JsonSerialize(using = ToStringSerializer.class)
    Long retentionBytes;

    @JsonProperty(RETENTION_MS_CONFIG)
    @Min(-1)
    @JsonSerialize(using = ToStringSerializer.class)
    Long retentionMs;

    @JsonProperty(SEGMENT_BYTES_CONFIG)
    @Min(14)
    Integer segmentBytes;

    @JsonProperty(SEGMENT_INDEX_BYTES_CONFIG)
    @Min(0)
    Integer segmentIndexBytes;

    @JsonProperty(SEGMENT_JITTER_MS_CONFIG)
    @Min(0)
    @JsonSerialize(using = ToStringSerializer.class)
    Long segmentJitterMs;

    @JsonProperty(SEGMENT_MS_CONFIG)
    @Min(1)
    @JsonSerialize(using = ToStringSerializer.class)
    Long segmentMs;

    @JsonProperty(UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG)
    Boolean uncleanLeaderElectionEnable;

    @JsonProperty(MESSAGE_DOWNCONVERSION_ENABLE_CONFIG)
    Boolean messageDownconversionEnable;

    public static TopicConfiguration getTopicConfigurationFromMap(Map<String, String> config) {
        TopicConfigurationBuilder topicConfigurationBuilder = TopicConfiguration.builder();
        topicConfigurationBuilder.cleanupPolicy(config.get(CLEANUP_POLICY_CONFIG));
        topicConfigurationBuilder.compressionType(config.get(COMPRESSION_TYPE_CONFIG));
        topicConfigurationBuilder.deleteRetentionMs(config.containsKey(DELETE_RETENTION_MS_CONFIG) ? Long.parseLong(config.get(DELETE_RETENTION_MS_CONFIG)) : null);
        topicConfigurationBuilder.fileDeleteDelayMs(config.containsKey(FILE_DELETE_DELAY_MS_CONFIG) ? Long.parseLong(config.get(FILE_DELETE_DELAY_MS_CONFIG)) : null);
        topicConfigurationBuilder.flushMessages(config.containsKey(FLUSH_MESSAGES_INTERVAL_CONFIG) ? Long.parseLong(config.get(FLUSH_MESSAGES_INTERVAL_CONFIG)) : null);
        topicConfigurationBuilder.flushMs(config.containsKey(FLUSH_MS_CONFIG) ? Long.parseLong(config.get(FLUSH_MS_CONFIG)) : null);
        topicConfigurationBuilder.followerReplicationThrottledReplicas(config.get(FOLLOWER_REPLICATION_THROTTLED_REPLICAS));
        topicConfigurationBuilder.indexIntervalBytes(config.containsKey(INDEX_INTERVAL_BYTES_CONFIG) ? Integer.parseInt(config.get(INDEX_INTERVAL_BYTES_CONFIG)) : null);
        topicConfigurationBuilder.leaderReplicationThrottledReplicas(config.get(LEADER_REPLICATION_THROTTLED_REPLICAS));
        topicConfigurationBuilder.maxMessageBytes(config.containsKey(MAX_MESSAGE_BYTES_CONFIG) ? Integer.parseInt(config.get(MAX_MESSAGE_BYTES_CONFIG)) : null);
        topicConfigurationBuilder.messageFormatVersion(config.get(MESSAGE_FORMAT_VERSION_CONFIG));
        topicConfigurationBuilder.messageTimestampDifferenceMaxMs(config.containsKey(MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG) ? Long.parseLong(config.get(MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG)) : null);
        topicConfigurationBuilder.messageTimestampType(config.get(MESSAGE_TIMESTAMP_TYPE_CONFIG));
        topicConfigurationBuilder.minCleanableDirtyRatio(config.containsKey(MIN_CLEANABLE_DIRTY_RATIO_CONFIG) ? Double.parseDouble(config.get(MIN_CLEANABLE_DIRTY_RATIO_CONFIG)) : null);
        topicConfigurationBuilder.minCompactionLagMs(config.containsKey(MIN_COMPACTION_LAG_MS_CONFIG) ? Long.parseLong(config.get(MIN_COMPACTION_LAG_MS_CONFIG)) : null);
        topicConfigurationBuilder.minInsyncReplicas(config.containsKey(MIN_IN_SYNC_REPLICAS_CONFIG) ? Integer.parseInt(config.get(MIN_IN_SYNC_REPLICAS_CONFIG)) : null);
        topicConfigurationBuilder.preallocate(config.containsKey(PREALLOCATE_CONFIG) ? Boolean.parseBoolean(config.get(PREALLOCATE_CONFIG)) : null);
        topicConfigurationBuilder.retentionBytes(config.containsKey(RETENTION_BYTES_CONFIG) ? Long.parseLong(config.get(RETENTION_BYTES_CONFIG)) : null);
        topicConfigurationBuilder.retentionMs(config.containsKey(RETENTION_MS_CONFIG) ? Long.parseLong(config.get(RETENTION_MS_CONFIG)) : null);
        topicConfigurationBuilder.segmentBytes(config.containsKey(SEGMENT_BYTES_CONFIG) ? Integer.parseInt(config.get(SEGMENT_BYTES_CONFIG)) : null);
        topicConfigurationBuilder.segmentIndexBytes(config.containsKey(SEGMENT_INDEX_BYTES_CONFIG) ? Integer.parseInt(config.get(SEGMENT_INDEX_BYTES_CONFIG)) : null);
        topicConfigurationBuilder.segmentJitterMs(config.containsKey(SEGMENT_JITTER_MS_CONFIG) ? Long.parseLong(config.get(SEGMENT_JITTER_MS_CONFIG)) : null);
        topicConfigurationBuilder.segmentMs(config.containsKey(SEGMENT_MS_CONFIG) ? Long.parseLong(config.get(SEGMENT_MS_CONFIG)) : null);
        topicConfigurationBuilder.uncleanLeaderElectionEnable(config.containsKey(UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG) ? Boolean.parseBoolean(config.get(UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG)) : null);
        return topicConfigurationBuilder.build();
    }

    public static Map<String, String> toMap(TopicConfiguration topicConfiguration) {
        Map<String, String> topicConfigMap = new HashMap<>();
        ofNullable(topicConfiguration.getCleanupPolicy()).ifPresent(config -> topicConfigMap.put(CLEANUP_POLICY_CONFIG, config));
        ofNullable(topicConfiguration.getCompressionType()).ifPresent(config -> topicConfigMap.put(COMPRESSION_TYPE_CONFIG, config));
        ofNullable(topicConfiguration.getDeleteRetentionMs()).ifPresent(config -> topicConfigMap.put(DELETE_RETENTION_MS_CONFIG, config.toString()));
        ofNullable(topicConfiguration.getFileDeleteDelayMs()).ifPresent(config -> topicConfigMap.put(FILE_DELETE_DELAY_MS_CONFIG, config.toString()));
        ofNullable(topicConfiguration.getFlushMessages()).ifPresent(config -> topicConfigMap.put(FLUSH_MESSAGES_INTERVAL_CONFIG, config.toString()));
        ofNullable(topicConfiguration.getFlushMessages()).ifPresent(config -> topicConfigMap.put(FLUSH_MS_CONFIG, config.toString()));
        ofNullable(topicConfiguration.getIndexIntervalBytes()).ifPresent(config -> topicConfigMap.put(INDEX_INTERVAL_BYTES_CONFIG, config.toString()));
        ofNullable(topicConfiguration.getMaxMessageBytes()).ifPresent(config -> topicConfigMap.put(MAX_MESSAGE_BYTES_CONFIG, config.toString()));
        ofNullable(topicConfiguration.getMessageFormatVersion()).ifPresent(config -> topicConfigMap.put(MESSAGE_FORMAT_VERSION_CONFIG, config));
        ofNullable(topicConfiguration.getMessageTimestampDifferenceMaxMs()).ifPresent(config -> topicConfigMap.put(MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG, config.toString()));
        ofNullable(topicConfiguration.getMessageTimestampType()).ifPresent(config -> topicConfigMap.put(MESSAGE_TIMESTAMP_TYPE_CONFIG, config));
        ofNullable(topicConfiguration.getMinCleanableDirtyRatio()).ifPresent(config -> topicConfigMap.put(MIN_CLEANABLE_DIRTY_RATIO_CONFIG, config.toString()));
        ofNullable(topicConfiguration.getMinCompactionLagMs()).ifPresent(config -> topicConfigMap.put(MIN_COMPACTION_LAG_MS_CONFIG, config.toString()));
        ofNullable(topicConfiguration.getMinInsyncReplicas()).ifPresent(config -> topicConfigMap.put(MIN_IN_SYNC_REPLICAS_CONFIG, config.toString()));
        ofNullable(topicConfiguration.getPreallocate()).ifPresent(config -> topicConfigMap.put(PREALLOCATE_CONFIG, config.toString()));
        ofNullable(topicConfiguration.getRetentionBytes()).ifPresent(config -> topicConfigMap.put(RETENTION_BYTES_CONFIG, config.toString()));
        ofNullable(topicConfiguration.getRetentionMs()).ifPresent(config -> topicConfigMap.put(RETENTION_MS_CONFIG, config.toString()));
        ofNullable(topicConfiguration.getSegmentBytes()).ifPresent(config -> topicConfigMap.put(SEGMENT_BYTES_CONFIG, config.toString()));
        ofNullable(topicConfiguration.getSegmentIndexBytes()).ifPresent(config -> topicConfigMap.put(SEGMENT_INDEX_BYTES_CONFIG, config.toString()));
        ofNullable(topicConfiguration.getSegmentJitterMs()).ifPresent(config -> topicConfigMap.put(SEGMENT_JITTER_MS_CONFIG, config.toString()));
        ofNullable(topicConfiguration.getSegmentMs()).ifPresent(config -> topicConfigMap.put(SEGMENT_MS_CONFIG, config.toString()));
        ofNullable(topicConfiguration.getUncleanLeaderElectionEnable()).ifPresent(config -> topicConfigMap.put(UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, config.toString()));
        ofNullable(topicConfiguration.getMessageDownconversionEnable()).ifPresent(config -> topicConfigMap.put(MESSAGE_DOWNCONVERSION_ENABLE_CONFIG, config.toString()));
        return topicConfigMap;
    }

    public static Map<String, String> getConfigDescription() {
        return new HashMap<>() {{
            put(CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DOC);
            put(COMPRESSION_TYPE_CONFIG, TopicConfig.COMPRESSION_TYPE_DOC);
            put(DELETE_RETENTION_MS_CONFIG, TopicConfig.DELETE_RETENTION_MS_DOC);
            put(FILE_DELETE_DELAY_MS_CONFIG, TopicConfig.FILE_DELETE_DELAY_MS_DOC);
            put(FLUSH_MESSAGES_INTERVAL_CONFIG, TopicConfig.FLUSH_MESSAGES_INTERVAL_DOC);
            put(FLUSH_MS_CONFIG, TopicConfig.FLUSH_MS_DOC);
            put(INDEX_INTERVAL_BYTES_CONFIG, TopicConfig.INDEX_INTERVAL_BYTES_DOCS);
            put(MAX_MESSAGE_BYTES_CONFIG, TopicConfig.MAX_MESSAGE_BYTES_DOC);
            put(MESSAGE_FORMAT_VERSION_CONFIG, TopicConfig.MESSAGE_FORMAT_VERSION_DOC);
            put(MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG, TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_DOC);
            put(MESSAGE_TIMESTAMP_TYPE_CONFIG, TopicConfig.MESSAGE_TIMESTAMP_TYPE_DOC);
            put(MIN_CLEANABLE_DIRTY_RATIO_CONFIG, TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_DOC);
            put(MIN_COMPACTION_LAG_MS_CONFIG, TopicConfig.MIN_COMPACTION_LAG_MS_DOC);
            put(MIN_IN_SYNC_REPLICAS_CONFIG, TopicConfig.MIN_IN_SYNC_REPLICAS_DOC);
            put(PREALLOCATE_CONFIG, TopicConfig.PREALLOCATE_DOC);
            put(RETENTION_BYTES_CONFIG, TopicConfig.RETENTION_BYTES_DOC);
            put(RETENTION_MS_CONFIG, TopicConfig.RETENTION_MS_DOC);
            put(SEGMENT_BYTES_CONFIG, TopicConfig.SEGMENT_BYTES_DOC);
            put(SEGMENT_INDEX_BYTES_CONFIG, TopicConfig.SEGMENT_INDEX_BYTES_DOC);
            put(SEGMENT_JITTER_MS_CONFIG, TopicConfig.SEGMENT_JITTER_MS_DOC);
            put(SEGMENT_MS_CONFIG, TopicConfig.SEGMENT_MS_DOC);
            put(UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_DOC);
            put(MESSAGE_DOWNCONVERSION_ENABLE_CONFIG, TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_DOC);
        }};
    }

}
