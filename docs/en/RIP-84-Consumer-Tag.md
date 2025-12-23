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

- **consumer-tag**: The label defined by the consumer for itself, not to the message. Different consumers within the same `consumer-group` can be configured with different tags.
- **consumer-tag-filter-conditions**: Filtering criteria defined by the consumer based on message attributes, determining which messages can match the current `consumer-tag`; this is different from the filtering expressions used in subscription.
- **message-push-ratio**: The proportion of messages that match the `consumer-tag-filter-conditions` and are delivered to the consumer.
- **allow-degraded-messages**: Whether the consumer accepts degraded messages. When set to true, the consumer will receive all messages, and the above three configuration items become ineffective.

**Constraints**

- Each consumer is allowed to have only one tag (consumer-tag).
- The `consumer-tag` can be configured at either the topic level or the client level.
- There must be a one-to-one correspondence between `consumer-tag` and `filter-conditions`; within the same consumer group, it is strictly prohibited to have one `consumer-tag` associated with multiple `filter-conditions`.
- Consumers without a tag configuration are considered **default consumers**, assigned a default tag and permitted to **receive degraded messages.**
- Only pop consumers are supported.


### broker-side

#### PopMessage

Add a message caching mechanism to `MessageStore`. (This is not the final implementation.)

The cache can be stored either in memory or in RocksDB, 
and **persistence is not required**—even if the Broker crashes and the cache is lost, 
data integrity is still guaranteed because messages can be re-consumed from the `commit offset` after restart.

The cache uses a hierarchical key-value structure, where:

- **Key:** A string with hierarchical semantics, facilitating efficient organization and retrieval by dimensions. The format is `consumer-group/tag/topic/queue`.
- **Value:** A size-bounded message queue (`Queue<Message>`), with a configurable maximum size (`maxSize`) to prevent unbounded cache growth.

  <div style="text-align: center">
    <img height="450" src="../cn/image/rip-84/message-cache.png" alt="message-cache">
  </div>

<br/>

**Message fetching logic**

  <div style="text-align: center">
     <img height="800" alt="pop-message" src="../cn/image/rip-84/pop-message.png" />
  </div>

1. **Prefer reading messages from cache:** Pull messages from the cache based on the current consumer’s consumer-tag. If the number of messages already read reaches maxMsgNums, immediately return the response.

2. **Prevent excessive backlog:** Calculate the difference between the current `read-offset` and the `commit-offset`. If this difference exceeds the configurable threshold MaxPendingMessages, stop reading new messages from the commitLog. This mechanism manifests as "message backlog" on the client side.

3. **Apply subscription filters:** When reading messages from the commitLog, first apply subscription filters. Messages that do not match are skipped, and the next message is read.

4. **Tag matching and cache insertion:**

  - If a message matches the current consumer’s consumer-tag, include it in the response.
  - If it does not match, iterate through all registered tag-filter rules within the same consumer group to assign an appropriate tag to the message and write it into the cache.
  - If no tag matches the message, mark it as default and write it into the cache.
   
  **Additional notes:**
    - **Normal scenario:** A default-consumer exists that can accept fallback messages and consume all messages marked as default.
    - **Abnormal scenario:** If the default-consumer is down or not deployed, messages that match no tag will still be labeled as default and continuously written to the cache. Once the cache reaches its capacity limit, subsequent consumption will be blocked, creating backpressure.

<br/>

**Here are several examples：**

1. **Normal scenario**, Consumption is handled solely by the default-consumer, and in this case, the cache will not be utilized.

    <div style="text-align: center">
        <img height="200" src="../cn/image/rip-84/only-default-consumer.png" alt="only-default-consumer">
    </div>

2. **Start of gray release**, Both the default-consumer and tag-consumer coexist.

    <div style="text-align: center">
        <img height="500" src="../cn/image/rip-84/gray-consume.png" alt="gray-consume">
    </div>

3. **Severe lag in gray consumer consumption**, leading to blockages in the consumption of messages tagged differently.

    <div style="text-align: center">
        <img height="500" src="../cn/image/rip-84/gray-consumer-block.png" alt="gray-consumer-block">    
    </div>

4. **Gray consumer crashes or offline**: Messages within the cache are taken over and consumed by the default-consumer.

    <div style="text-align: center">
        <img height="500" src="../cn/image/rip-84/gray-consumer-offline.png" alt="gray-consumer-offline">
    </div>

<br/>

**QA**

  **Q:** In the event of consumer failures, how can we ensure no message loss and minimize duplicates?
    
  **A:** If a consumer crashes, its associated messages will continue to be consumed by the default consumer. In the absence of a default consumer, consumption will be blocked once the cache reaches its upper limit. During this period, no messages will be lost, nor will there be any duplicate consumption.
    
  **Q:** If a single consumer gets stuck or falls behind, could its associated cache grow indefinitely?
    
  **A:** The cache has size limitations and will not grow indefinitely. A lagging consumer will cause the cache to reach its maximum capacity, blocking further consumption. From the user's perspective, this manifests as message backlog.

<br/><br/>

#### Maintain consumer metadata

We can maintain metadata within `ConsumerGroupInfo` using a key-value structure, where keys are stored hierarchically. 
Examples include:

- consumer-group/topic/tag: {"filter": "", "ratio": ""}
- consumer-group/topic/tag: ["consumer-A", "consumer-B", "consumer-xx"]

The broker side will have this data：

| tag   | filter | ratio | allow degraded msg | consumer    |
   |-------|--------|-------|--------------------|-------------|
| tag-A | v=2.0  | 0.5   | false              | consumer-A1 |
| tag-A | v=2.0  | 0.5   | false              | consumer-A1 |
| tag-B | env=qa | 1     | false              | consumer-B1 |
| /     | /      | /     | true               | consumer-C1 |

<br/>

**Consumer Events**

1. Consumer Registration
  - If a default-consumer already exists: only update metadata.
  - If no default-consumer exists:
    - If the registering consumer uses the default-tag or existing tag: only update metadata.
    - If the registering consumer uses a new tag: in addition to updating metadata, iterate through all messages in the default-tag cache, and move any messages matching the new tag into the corresponding new tag’s cache.
2. Consumer Deregistration / Crash
  - If other consumers with the same tag are still active: only update metadata.
  - If no other consumers with the same tag remain:
    - If the crashed consumer is the default-consumer: only update metadata, as messages under the default-tag are intended as a fallback and do not require migration.
    - If the crashed consumer is a non-default consumer: in addition to updating metadata, migrate all messages from that tag’s cache into the default-tag cache.

**Note:** During implementation, there is no need to physically move messages. Instead, simply mark that messages in a tag's cache can be handled by other tags, and let the consumption logic dynamically identify and route them at read time.

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

2. Add a property to `SimpleConsumerImpl`, `PushConsumerImpl`, `SimpleSubscriptionSettings`, and `PushSubscriptionSettings`.

    ```java
    private Map<String /* topic */, ConsumerTag> consumerTags;
    ```

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
    Map<String/*topic*/, Map<String/*tag*/, ConsumerTag>> tagTable;
    Map<String/*topic*/, Map<String/*tag*/, Set<clientId>>> clientTagTable;
    ```

2. Add PathTrie（java Trie）

   ```java
   public class PathTrie<T> {

         private static class Node<T> {
             final ConcurrentHashMap<String, Node<T>> children = new ConcurrentHashMap<>();
             volatile Queue<T> valueQueue;
             boolean isLeaf() {
                 return children.isEmpty();
             }
         }
     
         private final Node<Message> root = new Node<>();
         
         public void put(String path, T item) {}
         
         public Queue<Message> getQueue(String path) {}
         
         public List<Node<T>> getAllNodeUnderPrefix(String prefixPath) {}
         
         private Node<T> findNode(String path) {}
   }
   ```  

3. Add a cache field in MessageStore. Changes to the getMessage method are omitted here.

   ```java
   /* consumer-group/tag/topic/queue : Queue<Message> */
   PathTrie<Message> messageCache;
   ```


# Compatibility, Deprecation, and Migration Plan

  The new feature is forward-compatible, requires no migration, and does not deprecate any existing APIs.

# Rejected Alternatives

**There are two approaches to support canary deployment:**

1. **Create new consumer group**, This approach faces two boundary issues—duplicate consumption and potential message loss.
   - When an application is deployed in a canary environment, if the `subscription` of the base environment are not modified, the resulting massive duplicate consumption is often unacceptable. However, determining the right time to update the base environment’s `subscription` presents a significant challenge.
     - **Online**: The offset of the base environment must be greater than canary environment. This ensures only minimal duplicate consumption during switchover; otherwise, messages may be lost. This requirement forces the deployment system to be aware of consumption progress and handle anomalies such as message backlog.
     - **Offline**: The offset of the canary environment must be greater than base environment. This ensures only minimal duplicate consumption during switchover; otherwise, messages may be lost. However, the consumption rate in the canary environment cannot be guaranteed, if it’s too slow, scaling operations may be needed to catch up before offline.

2. Queue-isolation-based, Refer to: [#8468](https://github.com/apache/rocketmq/issues/8468)
   - This solution addresses the boundary issues more effectively but still lacks sufficient flexibility.

<br/>

**Advantages of the Consumer Tag**

1. **Canary testing is independent of producers** 
   
   Scenario example: A topic has one producer and multiple consumers. At a given time, both consumer-app-A and consumer-app-B are undergoing testing—app-A needs to validate traffic from Tenant A, while app-B needs to validate traffic from Tenant B. 

   With the Consumer Tag, you simply configure distinct tags and filters for app-A and app-B. This avoids duplicate consumption and eliminates boundary issues. In contrast, the aforementioned grayscale strategies would conflict in this scenario.

2. **Supports multi-environment isolation**

   Scenario example: User-A and User-B are testing different features of the same application. 
   
   they can deploy their applications with unique tags and filters, enabling isolated testing environments without interference.

3. **Supports proportional routing**
   
   Consumers can opt to consume only 1% of all messages, or—after applying a filter—consume 1% of the messages that match the filter criteria.
      
4. **Messages support fallback consumption** 

   When a specific consumer-tag is present, messages are consumed by the corresponding consumer. If no matching consumer exists, messages are automatically routed to the default-consumer. This ensures no message loss and no duplicate consumption.