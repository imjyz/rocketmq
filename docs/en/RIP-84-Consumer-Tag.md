# Status
- Current State: Proposed
- Authors: [imjyz](https://github.com/imjyz)
- Mailing List discussion: dev@rocketmq.apache.org

# Background & Motivation

## What do we need to do

- Will we add a new module?
  
  No.

- Will we add new APIs?
  
  No, we will modify the consumer configuration.

- Will we add new feature?
  
  Yes, consumer-based routing capabilities will be added under the same subscription.

## Why should we do that

- Are there any problems of our current project?   

  Yes, in RocketMQ, all consumers within a consumer group evenly consume messages from the same topic queues according to the subscription. 

  However, in certain scenarios, there is a need for different consumers to process different subsets of messages — for example, in multi-environment deployments or canary releases.

  While this can be achieved by using separate consumer groups, doing so increases operational complexity and reduces flexibility, making it difficult to implement dynamic routing or graceful fallback strategies.


- What can we benefit proposed changes?

  More flexible consumption, natively supporting canary releases and multi-environment deployments.


# Goals
- What problem is this proposal designed to solve?
  
  More flexible consumption, natively supporting canary releases and multi-environment deployments.

- To what degree should we solve the problem?

  Allows users to dynamically configure on the client side, but cannot be specified through the broker's interface. because only client-side implementation enables dynamic discovery.

# Non-Goals
- What problem is this proposal NOT designed to solve?
  
  Replacing the need for topic isolation in multi-tenant scenarios.

- Are there any limits of this proposal?

  Only pop consumers are supported.


# Changes

## Architecture

### client-side

**The consumer client supports the following configurable properties:**

- **consumer-tag**: The label defined by the consumer client for itself, not to the message. Different consumers within the same `consumer-group` can be configured with different tags.
- **consumer-tag-filter-conditions**: Filtering criteria defined by the consumer client based on message attributes, determining which messages can match the current `consumer-tag`; this is different from the filtering expressions used in subscription.
- **message-push-ratio**: The proportion of messages that match the `consumer-tag-filter-conditions` and are delivered to the client.
- **allow-degraded-messages**: Whether the consumer client accepts degraded messages. When set to true, the client will receive all messages, and the above three configuration items become ineffective.

**Constraints**

- Each consumer is allowed to have only one tag (consumer-tag).
- There must be a one-to-one correspondence between `consumer-tag` and `filter-conditions`; within the same consumer group, it is strictly prohibited to have one `consumer-tag` associated with multiple `filter-conditions`.
- Consumers without a tag configuration are considered **default consumers**, assigned a default tag and permitted to receive degraded messages.
- Only pop consumers are supported.
- The `consumer-tag` can be configured at either the topic level or the client level.


### broker-side


1. **Maintain client metadata**

    We could maintain metadata within ConsumerGroupInfo, with a hierarchical structure as follows: `/consumer-group/topic/tag/consumer`

    The broker side will have this data：

    | tag   | filter | ratio | allow degraded msg | consumer    |
    |-------|--------|-------|--------------------|-------------|
    | tag-A | v=2.0  | 0.5   | false              | consumer-A1 |
    | tag-A | v=2.0  | 0.5   | false              | consumer-A1 |
    | tag-B | env=qa | 1     | false              | consumer-B1 |
    | /     | /      | /     | true               | consumer-C1 |



2. **PopMessage**

    > The cache can be implemented within the MessageStore layer, or placed in a dedicated intermediate layer between PopMessageProcessor and MessageStore.

    MessageStore maintains a cache with the structure: `/consumer-group/topic/queue-id/tag/messages`. When a consumer pulls messages, it first reads from the cache; if the cached messages are insufficient, additional messages are fetched from the commitLog.

    MessageStore sequentially reads from the commitLog. Messages are first filtered by `subscription filter rules`, and then assigned a consumer-tag.
   
    <img width="1359" height="528" alt="image" src="https://github.com/user-attachments/assets/dbfc1ad4-b7a3-4b7d-baef-1a5aecac0996" />


    **Here are several examples:**

    **case 1**: consumer-A1 pulls messages
   
      - MessageStore first retrieves messages from the cache.
      - MessageStore first checks the cache. On miss or insufficiency, it reads messages sequentially from the commitLog on disk, skipping those that do not match the `subscription filter rules`.
      - It first tries to match tag-A (since the consumer is consumer-A). If matched, the message is added to consumer-A1's pull result and the next message is read; otherwise, it proceeds.
      - Iterate through all tags; if a message matches a tag, cache it under that tag. For consumers with downgrade enabled, the message is guaranteed to match exactly one tag.
      - If a message does not match any tag, it is marked as a failed delivery and retried. After the retry count exceeds max-retry, the message is moved to the dead letter queue (DLQ).


    **case 2**: Default-consumer pulls messages

      &nbsp;&nbsp; Messages are read from the cache first, including those tagged as default and tags with no active consumers. When reading from the commit-log, the system first attempts to find a matching tag; the message is delivered to the Default subscriber only if no match is found.


    **case 3**：tag-A messages are cached, but all its consumers are offline.

      &nbsp;&nbsp; If a Default-consumer exists, the message will be consumed by it. Otherwise, Retry until DLQ.

3. **Expected outcome**

   **Scenario without consumer-tag**
  
   <img width="804" height="278" alt="Image" src="https://github.com/user-attachments/assets/3121d4f2-17ea-4990-9100-b369b469317f" />

   **Scenario with consumer-tag**

   <img width="781" height="353" alt="Image" src="https://github.com/user-attachments/assets/ff17d7dd-7cb1-46bf-b0a4-2da50a3b7811" />


## Interface Design/Change


### client-side

1. Add a configuration class:
   
    ```java
    public class ConsumerTag {
        private String tag;
        private String filterConditions;
        private double messagePushRatio = 1.0d;
        private Boolean allowDegradedMessages = Boolean.TRUE;
    }
    ```

2. Add a property `private Map<String /* topic */, ConsumerTag> consumerTags;` to `SimpleConsumerImpl`, `PushConsumerImpl`, `SimpleSubscriptionSettings`, and `PushSubscriptionSettings`.


3. Add a method to  `SimpleConsumerImpl` and `PushConsumerImpl`:
   
    ```java
    public SimpleConsumer subscribe(String topic, FilterExpression filterExpression, ConsumerTag consumerTag) throws ClientException{
        consumerTags.put(topic, consumerTag);
        ...
    }
    ```

### broker-side

1. Add fields to `ConsumerGroupInfo` to maintain metadata for consumer tags.

   ```java
    private ConcurrentMap<String/* Topic */, Set<String/* tag */>> tagTable;
    private ConcurrentMap<String/* topic@tag */, ConsumerTag> consumerTagTable;
    private ConcurrentMap<String/* clientId */, String/* topic@tap */> clientTagTable;
   ```

2. Some logic in PopMessageProcessor has been modified; details are not covered here.   


# Compatibility, Deprecation, and Migration Plan

  The new feature is forward-compatible, requires no migration, and does not deprecate any existing APIs.

# Rejected Alternatives

  Reference: [#8468](https://github.com/apache/rocketmq/issues/8468)
  
  This approach requires producers to cooperate in tagging, involves heavier publishing overhead, and cannot flexibly support multiple environments.
