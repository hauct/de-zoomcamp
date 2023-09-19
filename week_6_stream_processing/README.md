# Week 6: Stream Processing

## Materials

See [Week 6: Stream
Processing](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_6_stream_processing), particularly
this
[README](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_6_stream_processing/python/README.md)
on GitHub.

Youtube videos:

- [DE Zoomcamp 6.0.1 - Introduction](https://www.youtube.com/watch?v=hfvju3iOIP0)
- [DE Zoomcamp 6.0.2 - What is stream processing](https://www.youtube.com/watch?v=WxTxKGcfA-k)
- [DE Zoomcamp 6.3 - What is Kafka?](https://www.youtube.com/watch?v=zPLZUDPi4AY)
- [DE Zoomcamp 6.4 - Confluent cloud](https://www.youtube.com/watch?v=ZnEZFEYKppw)
- [DE Zoomcamp 6.5 - Kafka producer consumer](https://www.youtube.com/watch?v=aegTuyxX7Yg)
- [DE Zoomcamp 6.6 - Kafka configuration](https://www.youtube.com/watch?v=SXQtWyRpMKs)
- [DE Zoomcamp 6.7 - Kafka streams basics](https://www.youtube.com/watch?v=dUyA_63eRb0)
- [DE Zoomcamp 6.8 - Kafka stream join](https://www.youtube.com/watch?v=NcpKlujh34Y)
- [DE Zoomcamp 6.9 - Kafka stream testing](https://www.youtube.com/watch?v=TNx5rmLY8Pk)
- [DE Zoomcamp 6.10 - Kafka stream windowing](https://www.youtube.com/watch?v=r1OuLdwxbRc)
- [DE Zoomcamp 6.11 - Kafka ksqldb & Connect](https://www.youtube.com/watch?v=DziQ4a4tn9Y)
- [DE Zoomcamp 6.12 - Kafka Schema registry](https://www.youtube.com/watch?v=tBY_hBuyzwI)

## Notice

My notes below follow the sequence of the videos and topics presented in them.

## 6.0.1 Introduction to Stream Processing

We will cover in week 6 :

- What is stream processing?
- What is Kafka
- Stream processing message properties
- Kafka setup and configuration
- Time spanning in stream processing
- Kafka producer and Kafka consumer
- Partitions
- How to work with Kafka streams
- Schema and its role in flow processing
- Kafka Connect
- ksqlDB

## 6.0.2 What is stream processing?

**Stream processing** is a data management technique that involves ingesting a continuous data stream to quickly
analyze, filter, transform or enhance the data in real time.

I recommend reading [Introduction to streaming for data
scientists](https://huyenchip.com/2022/08/03/stream-processing-for-data-scientists.html) by Chip Huyen.

### Data exchange

Data exchange allows data to be shared between different computer programs.

### Producer and consumers

More generally, a producer can create messages that consumers can read. The consumers may be interested in certain topics. The producer indicates the topic of his messages. The consumer can subscribe to the topics of his choice.

### Data exchange in stream processing

When the producer posts a message on a particular topic, consumers who have subscribed to that topic receive the message
in real time.

Real time does not mean immediately, but rather a few seconds late, or more generally much faster than batch processing.

## 6.3 What is Kafka?

<img src="images/kafka.png" width="400">

[Apache Kafka](https://kafka.apache.org/Kafka) Apache Kafka is an open-source distributed event streaming platform for
high-performance data pipelines, streaming analytics, data integration, and mission-critical applications. Kafka
provides a high-throughput and low-latency platform for handling real-time data feeds.

Kafka runs as a cluster in one or more servers. The Kafka cluster stores streams of records in categories called topics.
Each record has a key, value, and a timestamp.

It was originally developed at LinkedIn and was subsequently open-sourced under the Apache Software Foundation in 2011.
It’s widely used for high-performance use cases like streaming analytics and data integration.

See [org.apache.kafka](https://javadoc.io/doc/org.apache.kafka) Javadocs.

### Kafka

In this section, you will find my personal notes that I had taken before this course.

These notes come from:

- Effective Kafka, by Emil Koutanov
- Kafka: The Definitive Guide, 2nd Edition, by Gwen Shapira, Todd Palino, Rajini Sivaram, Krit Petty
- [Kafka: A map of traps for the enlightened dev and op](https://www.youtube.com/watch?v=paVdXL5vDzg&t=1s) by Emmanuel
  Bernard And Clement Escoffier on Youtube.

#### Overview

Kafka is a distributed system comprising several key components. These are four main parts in a Kafka system:

- **Broker**: Handles all requests from clients (produce, consume, and metadata) and keeps data replicated within the
  cluster. There can be one or more brokers in a cluster.
- **Zookeeper** (now **KRaft**): Keeps the state of the cluster (brokers, topics, users).
- **Producer**: Sends records to a broker.
- **Consumer**: Consumes batches of records from the broker.

A **record** is the most elemental unit of persistence in Kafka. In the context of event-driven architecture, a record
typically corresponds to some event of interest. It is characterised by the following attributes:

- **Key**: A record can be associated with an optional non-unique key, which acts as a king of classifier, grouping
  relatied records on the basis of their key.
- **Value**: A value is effectively the informational payload of a record.
- **Headers**: A set of free-form key-value pairs that can optionally annotate a record.
- **Partition number**: A zero-based index of the partition that the record appears in. A record must always be tied to
  exactly one partition.
- **Offset**: A 64-bit signed integer for locating a record within its encompassing partition.
- **Timestamp**: A millisecond-precise timestamp of the record.

A **partition** is a totally ordered, unbounded set of records. Published records are appended to the head-end of the
encompassing partition. Where a record can be seen as an elemental unit of persistence, a partition is an elemental unit
of record streaming. In the absence of producer synchronisation, causal order can only be achieved when a single
producer emits records to the same partition.

A **topic** is a logical aggregation of partitions. It comprises one or more partitions, and a partition must be a part
of exactly one topic. Earlier, it was said that partitions exhibit total order. Taking a set-theoretic perspective, a
topic is just a union of the individual underlying sets; since partitions within a topic are mutually independent, the
topic is said to exhibit partial order. In simple terms, this means that certain records may be ordered in relation to
one another, while being unordered with respect to certain other records.

A **cluster** hosts multiple topics, each having an assigned leader and zero or more follower replicas.

![p160](images/kafka-flow.png)

#### Main Concepts

See [Streams Concepts](https://docs.confluent.io/platform/current/streams/concepts.html#streams-concepts).

- **Publish/subscribe messaging** is a pattern that is characterized by the sender (publisher) of a piece of data
  (message) not specifically directing it to a receiver.
- These systems often have a **broker**, a central point where messages are published, to facilitate this pattern.
- The unit of data within Kafka is called a **message**.
- A message can have an optional piece of metadata, which is referred to as a **key**.
- While messages are opaque byte arrays to Kafka itself, it is recommended that additional structure, or **schema**, be
  imposed on the message content so that it can be easily understood.
- Messages in Kafka are categorized into **topics**. The closest analogies for a topic are a database table or a folder
  in a filesystem.
- Topics are additionally broken down into a number of **partitions**.
- A **stream** is considered to be a single topic of data, regardless of the number of partitions, moving from the
  producers to the consumers.
- **Producers** create new messages. In other publish/subscribe systems, these may be called **publishers** or
  **writers**.
- **Consumers** read messages. In other publish/subscribe systems, these clients may be called **subscribers** or
  **readers**.
- The consumer keeps track of which messages it has already consumed by keeping track of the **offset** of messages. The
  **offset**, an integer value that continually increases, is another piece of metadata that Kafka adds to each message
  as it is produced.
- Consumers work as part of a **consumer group**, which is one or more consumers that work together to consume a topic.
- A single Kafka server is called a **broker**. The broker receives messages from producers, assigns offsets to them,
  and writes the messages to storage on disk.
- Kafka brokers are designed to operate as part of a **cluster**.
- Within a **cluster of brokers**, one broker will also function as the cluster **controller** (elected automatically
  from the live members of the cluster).
- A partition is owned by a single broker in the cluster, and that broker is called the **leader** of the partition
- A replicated partition is assigned to additional brokers, called **followers** of the partition.

**Replication of partitions in a cluster**

![p161](images/kafka-replication.png)

#### Kafka is simple…​

**Kafka is simple**

![p162](images/kafka-simple.png)

This picture comes from [Kafka: A map of traps for the enlightened dev and
op](https://www.youtube.com/watch?v=paVdXL5vDzg&t=1s) by Emmanuel Bernard And Clement Escoffier on Youtube.

#### Installation

We can install Kafka locally.

If you have already installed Homebrew for macOS, you can use it to install Kafka in one step. This will ensure that you
have Java installed first, and it will then install Apache Kafka 2.8.0 (as of the time of writing).

``` bash
$ brew install kafka
```

Homebrew will install Kafka under `/opt/homebrew/Cellar/kafka/`.

But, in this course, we use [Confluent Cloud](https://www.confluent.io/confluent-cloud/). Confluent cloud provides a
free 30 days trial for, you can signup [here](https://www.confluent.io/confluent-cloud/tryfree/).

### Topic

Topic is a container stream of events. An event is a single data point in timestamp.

Multiple producers are able to publish to a topic, picking a partition at will. The partition may be selected directly —
by specifying a partition number, or indirectly — by way of a record key, which deterministically hashes to a partition
number.

Each topic can have one or many consumers which subscribe to the data written to the topic.

### Partition

A Kafka Topic is grouped into several partitions for scalability. Each partition is an sequence of records that are
continually added to a structured commit log. A sequential ID number called the offset is assigned to each record in the
partition.

### Logs

Kafka logs are a collection of various data segments present on your disk, having a name as that of a form-topic
partition or any specific topic-partition. Each Kafka log provides a logical representation of a unique topic-based
partitioning.

Logs are how data is actually stored in a topic.

### Event

Each event contains a number of messages. A message has properties.

### Message

The basic communication abstraction used by producers and consumers in order to share information in Kafka is called a
**message**.

Messages have 3 main components:

- **Key**: used to identify the message and for additional Kafka stuff such as partitions (covered later).
- **Value**: the actual information that producers push and consumers are interested in.
- **Timestamp**: used for logging.

### Why Kafka?

**Kafka brings robustness**: For example, when a server goes down, we can still access the data. Apache Kafka achieves a
certain level of resiliency through replication, both across machines in a cluster and across multiple clusters in
multiple data centers.

**Kafka offers a lot of flexibility**: The data exchange application can be small or very large. Kafka can be connected
to multiple databases with Kafka connect

**Kafka provides scalability**: Kafka has no problem handling a number of events that increases dramatically in a short time.

### Availability of messages

When a consumer reads a message, that message is not lost and is still available to other consumers. There is some kind of expiration date for messages.

### Need of stream processing

Before, we often had monolithic applications. Now, we can have several microservices talking to each other. Kafka helps
simplify data exchange between these microservices

See also [What is Apache Kafka](https://kafka.apache.org/intro) for more.

## 6.4 Confluent cloud

### Create a free account

Go to <https://confluent.cloud/signup> and create a free account. You do not need to enter your credit card number.

### Confluent Cloud Interface

The first page you should see is this:

![p163](images/confluent-0.png)

Click on **Add Cluster** to create a cluster.

Click on **Begin configuration** button from the Free Basic option.

Select a **Region** near you (ideally offering low carbon intensity) and click on **Continue** button.

![p164](images/confluent-1.png)

In my case, i choose the same setting as My GCS , `Singapore (asia-southeast1)` and `Single zone`

You do not need to enter your credit card number. So, we can click on **Skip payment** button.

In **Create cluster** form, enter the **Cluster name** `kafka_tutorial_cluster` and click on **Lauch cluster** button.

![p165](images/confluent-2.png)

### Explore interface

After that you should see this:

![p166](images/confluent-cluster-interface.png)

Explore the different interfaces : Dashboard, Networking, API Keys, etc.

### API Keys

An API key consists of a key and a secret. Kafka API keys are required to interact with Kafka clusters in Confluent
Cloud. Each Kafka API key is valid for a specific Kafka cluster.

Click on **API Keys** and on **Create key** button.

Select **Global access** and click on **Next** button.

Enter the following description: `kafka_cluster_tutorial_api_key`.

![p167](images/kafka-api-key.png)

### Create a topic

A Topic is a category/feed name to which records are stored and published. All Kafka records are organized into topics.
Producer applications write data to topics and consumer applications read from topics. Records published to the cluster
stay in the cluster until a configurable retention period has passed by.

Select **Topics** in the left menu, and click on **Create topic** button.

In the **New topic form**, enter :

- **Topic name** : tutorial_topic
- **Partitions** : 2
- Click on **Show advanced settings**
- **Retention time**: 1 day

![p168](images/confluent-new-topic.png)

Click on **Save & create** button.

We should see this:

![p169](images/confluent-topic-created.png)

### Produce a new message

Now, we can produce some new messages.

Select the **Messages** tab, click on **+ Produce a new message to this topic**.

![p170](images/confluent-produce-message.png)

Click on **Produce** button.

The message produced has a **Value**, an empty **Header** and a **Key**.

I notice that we do not see certain fields of the message such as the partition, offset, timestamp.

### Create a connector

Confluent Cloud offers pre-built, fully managed Kafka connectors that make it easy to instantly connect your clusters to
popular data sources and sinks. Connect to external data systems effortlessly with simple configuration and no ongoing
operational burden.

Select **Connectors** in the left menu, and click on **Datagen Source**.

Select **tutorial_topic**.

![p171](images/datagen-source-connector-1.png)

Click on **Continue** button.

Select **Global access** and click on **Continue** button.

Under **Select output record value format**, select **JSON**. Under **Select a template**, select **Orders**. Click on
**Continue** button.

The instructor says that the **Connector sizing** is fine. Click on **Continue** button.

Change the **Connector name** for `OrdersConnector_tutorial`

![p172](images/datagen-source-connector-2.png)

Click on **Continue** button.

We should see this.

![p173](images/datagen-source-connector-3.png)

The connector is being provisioned. This may take 2 or 3 minutes.

Click on the **OrderConnector_tutorial** connector. You should see that the connector is active.

![p174](images/datagen-source-connector-4.png)

Now that the connector is up and running, let’s navigate back to the topics view to inspect the incoming message.

Click on **Explore metrics** button. We should see some thing like this. Take the time to explore and learn the
available metrics.

![175](images/confluent-explore-metrics.png)

### Return to the topic

Select the **tutorial_topic** that we just configured the connector to produce to, to view more details.

Under **Overview** tab, we see the production rate and consumption rate as bytes per second.

Under **Messages** tab, we see that a number of messages have been created.

<table>
<tr><td>
<img src="images/confluent-topic-messages1.png">
</td><td>
<img src="images/confluent-topic-messages2.png">
</td></tr>
</table>

### Shut down the connector

Select **Connectors** in the left menu, select our connector **OrdersConnector_tutorial**, and click on **Pause**
button.

We always have to stop processes at the end of a work session so we don’t burn our \$400 free credit dollars.

See also [Confluent Cloud](https://docs.confluent.io/cloud/current/overview.html) and [Confluent
Documentation](https://docs.confluent.io/home/overview.html).

## 6.5 Kafka producer consumer

### What we will cover

We will cover :

- Produce some messages programmaticaly
- Consume some data programmaticaly

We will use Java for this. If we want to use Python, there’s a Docker image to help us.





