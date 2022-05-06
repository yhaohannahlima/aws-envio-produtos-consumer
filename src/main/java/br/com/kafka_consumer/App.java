package br.com.kafka_consumer;

import java.io.FileNotFoundException;
import java.util.concurrent.ExecutionException;

import br.com.kafka_consumer.services.KafkaService;

public class App 
{
    public static void main( String[] args ) throws InterruptedException, ExecutionException, FileNotFoundException
    {
        System.out.println("Lendo mensagens ...");
        var grupoId = System.getenv("KAFKA_GROUP_ID_READER");
        KafkaService.readMessage(grupoId);
    }
}

