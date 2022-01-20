// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package io.streamnative.examples;

import com.beust.jcommander.JCommander;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Energy Producer
 *
 * TODO:   convert to function
 *         https://pulsar.apache.org/docs/en/functions-develop/
 */
public class EnergyProducer {

    // default
    private static String DEFAULT_TOPIC = "persistent://public/default/energy-influx";

    /**
     * schemas https://pulsar.apache.org/docs/en/schema-understand/
     * http://pulsar.apache.org/docs/en/concepts-schema-registry/
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
        Logger logger = LoggerFactory.getLogger(EnergyProducer.class);

        logger.debug("Available processors (cores): " +
                Runtime.getRuntime().availableProcessors());

        /* Total amount of free memory available to the JVM */
        logger.debug("Free memory (bytes): " +
                Runtime.getRuntime().freeMemory());

        /* This will return Long.MAX_VALUE if there is no preset limit */
        long maxMemory = Runtime.getRuntime().maxMemory();
        /* Maximum amount of memory the JVM will attempt to use */
        logger.debug("Maximum memory (bytes): " +
                (maxMemory == Long.MAX_VALUE ? "no limit" : maxMemory));

        /* Total memory currently available to the JVM */
        logger.debug("Total memory available to JVM (bytes): " +
                Runtime.getRuntime().totalMemory());

        JCommanderPulsar jct = new JCommanderPulsar();
        JCommander jCommander = new JCommander(jct, args);
        if (jct.help) {
            jCommander.usage();
            return;
        }
        if (jct.topic == null || jct.topic.trim().length() <= 0) {
            jct.topic = "persistent://public/default/energy-influx";
        }
        if (jct.serviceUrl == null || jct.serviceUrl.trim().length() <= 0) {
            jct.serviceUrl = "pulsar://pulsar1:6650";
        }

        logger.debug("topic:" + jct.topic);
        logger.debug("url:" + jct.serviceUrl);

        PulsarClient client = null;

        try {
            client = PulsarClient.builder().serviceUrl(jct.serviceUrl.toString()).build();
        } catch (PulsarClientException e) {
            logger.error(e.getLocalizedMessage());
        }

        String OS = System.getProperty("os.name").toLowerCase();
        String topic = DEFAULT_TOPIC;

        if (jct.topic != null && jct.topic.trim().length() > 0) {
            topic = jct.topic.trim();
        }
        ProducerBuilder<Device> producerBuilder = getProducerBuilder(client, topic);

        Consumer consumer = client.newConsumer()
                .topic("persistent://public/default/energy")
                .subscriptionName("energy-java-consumer")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        String pulsarKey = null;
        String energyString = null;
        Energy energyMsg = null;
        Producer<Device> producer = null;
        MessageId msgID = null;
        int messageCount = 0;

        // continuous messages
        while (true) {
            if (consumer == null || !consumer.isConnected()) {
                throw new Exception("Consumer is null");
            }

            Message msg = consumer.receive();
            messageCount += 1;

            try {
                consumer.acknowledge(msg);
                logger.debug("Acked message [" + msg.getMessageId() + "], Total messages acked so far: " + messageCount);
                logger.debug("Total Messages Received: " + consumer.getStats().getTotalMsgsReceived());

                if (msg == null || msg.getData() == null) {
                    throw new Exception("No message");
                }
                energyString = new String(msg.getData());
                energyMsg = parseMessage("" + energyString);

                logger.info("Receive message Current:" + energyMsg.getCurrent());
                logger.info("Receive message Power:" + energyMsg.getPower());
                logger.debug("Receive message JSON:" + energyMsg.toString());

                try {
                    producer = producerBuilder.create();
                } catch (PulsarClientException e) {
                    producerBuilder = getProducerBuilder(client, topic);
                    producer = producerBuilder.create();
                }
                if (producer == null) {
                    break;
                }
                pulsarKey = buildKey();
                msgID = producer.newMessage()
                        .key(pulsarKey)
                        .value(buildInfluxMessage("pulsar1", topic, energyMsg, OS))
                        .send();

                logger.info("Publish message ID " + msgID + " OS:" + OS + " Key:" + pulsarKey);
            } catch (Exception e) {
                consumer.negativeAcknowledge(msg);
                logger.error(msg.getMessageId() + " failed " + e.getLocalizedMessage());
            }

            if (producer != null)
                producer.close();
        }

        try {
            client.close();
        } catch (PulsarClientException e) {
            logger.error(e.getLocalizedMessage());
        }
        consumer = null;
        producer = null;
        producerBuilder = null;
        client = null;
    }

    /**
     * build producer
     *
     * @param client
     * @param topic
     * @return ProducerBuilder
     */
    private static ProducerBuilder<Device> getProducerBuilder(PulsarClient client, String topic) {
        return client.newProducer(JSONSchema.of(Device.class))
                .topic(topic)
                .producerName("jetson-java-energy")
                .sendTimeout(10, TimeUnit.SECONDS);
    }

    /**
     * build pulsar key
     **/
    private static String buildKey() {
        UUID uuidKey = UUID.randomUUID();
        String pulsarKey = uuidKey.toString();
        return pulsarKey;
    }

    /**
     * build device in influx db line format
     *
     * @return device
     */
    private static Device buildInfluxMessage(String host, String topic, Energy energy, String os) {
        Device device = new Device();
        if (host == null || topic == null || energy == null) {
            return device;
        }
        try {
            device.setMeasurement("current");
            device.timestamp = Instant.now().toEpochMilli();
            device.tags = Maps.newHashMap();
            device.tags.put("host", host);
            device.tags.put("topic", topic);
            device.tags.put("os", os);
            device.fields = Maps.newHashMap();
            device.fields.put("power",  String.format("%s",energy.getPower()));
            device.fields.put("value", String.format("%s",energy.getCurrent()));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return device;
    }

    /**
     * @param message String of message
     * @return IoTMessage
     */
    private static Energy parseMessage(String message) {
        Energy energyMessage = null;

        try {
            if (message != null && message.trim().length() > 0) {
                ObjectMapper mapper = new ObjectMapper();
                energyMessage = mapper.readValue(message, Energy.class);
                mapper = null;
            }
        } catch (Throwable t) {
            t.printStackTrace();
            energyMessage = null;
        }

        if (energyMessage == null) {
            energyMessage = new Energy();
        }
        return energyMessage;
    }
}
