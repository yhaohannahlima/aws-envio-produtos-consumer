# Sobre o kafka - https://kafka.apache.org/
- Plataforma de streaming distribuida (é além de um sistema de fila como o rabbitmq)
- É um banco de dados, armazena os dados (não faz os dados 100% em memória)
- Você pode configurar o período de exclusão das mensagens no kafka
- É super rápido e tem baixa latência
- Armazena os dados de forma distribuída entre os seus clusters elencando automaticamente as partições principais em cada cluster
- Processa mensagens em tempo real
- Tem por padrão o serviço de pub/sub através dos groupsId
- Ele não garante a ordem das mensagens (ordem é somente dentro de uma partição, porém em um sistema distribuido, ele recupera todas as mensagens de todas as partições, não garantindo a ordem)
- Por padrão você consegue enviar mensagens com o tamanho limite de 1MB, esta mudança pode ser feita no arquivo: server.properties em: message.max.bytes=20971520

## Install
- https://kafka.apache.org/downloads
- https://www.tutorialkart.com/apache-kafka/install-apache-kafka-on-mac/
```shell
vim ~/.bash_profile
# colocar no bash_profile:
alias "start_zookeeper= sh ~/kafka_2.13-3.1.0/bin/zookeeper-server-start.sh ~/kafka_2.13-3.1.0/config/zookeeper.properties" 
alias "start_kafka= sh ~/kafka_2.13-3.1.0/bin/kafka-server-start.sh ~/kafka_2.13-3.1.0/config/server.properties" 
source ~/.bash_profile
```

## Tópicos
- Grupo de mensagens armazenadas para que um consumidor possa ler
- https://trello.com/1/cards/625890fa16b7da62fc29240c/attachments/625895d3e940241f23e4a4c1/previews/625895d4e940241f23e4af10/download/image.png

## Zookeeper
- Sistema gerenciador das mensagens em partições por tópicos no Kafka
- https://trello.com/1/cards/625890fa16b7da62fc29240c/attachments/6258963548043814e3678fde/previews/6258963748043814e36791cf/download/image.png

## Cluster Kafica
- Um conjunto de maquinas distribuindo e dando performance a um kafka com balanciamento
- https://trello.com/1/cards/625890fa16b7da62fc29240c/attachments/625897ab96b90d8cc8571680/previews/625897ac96b90d8cc8571688/download/image.png
- https://trello.com/1/cards/625890fa16b7da62fc29240c/attachments/6258963548043814e3678fde/previews/6258963748043814e36791cf/download/image.png

## Brokers
- Servidores que são utilizados para armazenar/entrega as mensagens
- Ele armazena as mensagens em partições diferentes em cada Broker, deixando uma partição em cada Broker diferente
- https://trello.com/1/cards/625890fa16b7da62fc29240c/attachments/625897ab96b90d8cc8571680/previews/625897ac96b90d8cc8571688/download/image.png
- https://trello.com/1/cards/625890fa16b7da62fc29240c/attachments/625895d3e940241f23e4a4c1/previews/625895d4e940241f23e4af10/download/image.png

## Partitions
- Uma fila que armazena as mensagens organizadas por tópicos e armazenada entro de brokers
- https://trello.com/1/cards/625890fa16b7da62fc29240c/attachments/625890fd346bf94bbbf9473a/previews/625890fe346bf94bbbf9476d/download/image.png

## Replication Factor
- Define quantas replicas das partições você irá querer em cada Broker
- https://trello.com/1/cards/625890fa16b7da62fc29240c/attachments/625898768eaded26d17777d8/previews/625898778eaded26d17777e1/download/image.png
- Se deixarmos somente uma replica, caso perca um dos clusters, todas as mensagens daquele cluster serão perdidas
- Imagem: https://trello.com/1/cards/625890fa16b7da62fc29240c/attachments/62589929d46b7c4ba7bf396d/previews/6258992ad46b7c4ba7bf3983/download/image.png
- Quem faz este gerencimento é o Zookeeper

## Offset
- Posição da mensagem armazenada em uma partição
- https://trello.com/1/cards/625890fa16b7da62fc29240c/attachments/62589534282a97642aea084d/previews/62589535282a97642aea087c/download/image.png

## Producer
- Sistema que envia as mensagens

## Consumer
- Sistema que consome as mensagens

<hr>

# Criando projeto
mvn archetype:generate -DgroupId=br.com.kafka_producer -DartifactId=kafka_producer -DarchetypeArtifactId=maven-archetype-quickstart -DarchetypeVersion=1.4 -DinteractiveMode=false

# doc mvn - gerar com manifest
https://www.sohamkamani.com/java/cli-app-with-maven/


# como rodar
- configurar o seu .bash_profile ou .bashrc
```shell
code ~/.bash_profile

export KAFKA_HOST="192.168.0.19:9092"
export KAFKA_TOPIC="TOPICO_1"
export KAFKA_GROUP_ID_READER="consomer_1"

source ~/.bash_profile
```

# rodar o comando
```shell
./buld.sh
./start.sh
```


# enviando e consumindo mensagem via terminal
```shell
# lista topicos
~/kafka_2.13-3.1.0/bin/kafka-topics.sh --list --bootstrap-server=192.168.0.19:9092

# cria um topico
~/kafka_2.13-3.1.0/bin/kafka-topics.sh --create --bootstrap-server=192.168.0.19:9092 --replication-factor=1 --partitions=1 --topic="UM_TOPICO"

# enviando mensagem
~/kafka_2.13-3.1.0/bin/kafka-console-producer.sh --broker-list=192.168.0.19:9092 --topic="UM_TOPICO"
# irá abrir um console para você irá digitar vários valores, exemplo:
# > valor
# > valor

# consumingo mensagems
~/kafka_2.13-3.1.0/bin/kafka-console-consumer.sh --bootstrap-server=192.168.0.19:9092 --topic="UM_TOPICO" # consome somente as mesagens novas
~/kafka_2.13-3.1.0/bin/kafka-console-consumer.sh --bootstrap-server=192.168.0.19:9092 --topic="UM_TOPICO" --from-beginning # lê mensagens deste o inicio

# deletar mensagem de um tópico
echo '{ "partitions": [ { "topic": "UM_TOPICO", "partition": 0, "offset": 1 } ],  "version": 1 }' > delete-records.json
~/kafka_2.13-3.1.0/bin/kafka-delete-records.sh --bootstrap-server 192.168.0.19:9092 --offset-json-file delete-records.json

# deletar um topico
~/kafka_2.13-3.1.0/bin/kafka-topics.sh --topic="EXEMPLO_TOPICO" --delete --bootstrap-server=192.168.0.19:9092

```

# para criar um balanciamento de consumidores alterar o arquivo server.properties
```shell
vim ~/kafka_2.13-3.1.0/config/server.properties 
# alterar o valor: 
# num.partitions=3

# agora é alterar o tópico
~/kafka_2.13-3.1.0/bin/kafka-topics.sh --alter --bootstrap-server=192.168.0.19:9092 --topic=TOPICO_1 --partitions=3

# para verificar as partições de um tópido
~/kafka_2.13-3.1.0/bin/kafka-topics.sh --bootstrap-server=192.168.0.19:9092 --describe

# alterar o KafkaService.java
vim src/main/java/br/com/kafka_consumer/services/KafkaService.java
# acrescentar no metodo properties:
# properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()); // para enviar dados em consumidores diferentes
# properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1"); // para evitar conflito de partições e rebalanciamento

# agora 2 consumidores passam a trabalhar em paralelo
# para testar, rodar 2 consumidores:
# ./start.sh # terminal 1
# ./start.sh # terminal 2
```

