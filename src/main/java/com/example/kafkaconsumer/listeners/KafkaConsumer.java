package com.example.kafkaconsumer.listeners;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Service
public class KafkaConsumer {

    @Value("${message.file.path}")
    private String path;

    DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("dd-MM-yyyy_HH-mm-ss");

    @KafkaListener(topics = "#{'${server.kafka.topic.test}'}",
            groupId = "#{'${server.kafka.topic.group}'}",
            containerFactory = "kafkaListenerContainerFactory")
    public void consumeTestTopic(ConsumerRecord message) throws IOException {
        System.out.println("mothi>>>>>>>>" +message.value().toString());
        FileWriter file = new FileWriter(path+"/TestTopic-"+ LocalDateTime.now().format(FORMATTER)+".json");
        file.write(message.value().toString());
        file.close();
    }

}
