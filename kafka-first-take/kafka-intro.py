import asyncio
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from pydantic import BaseSettings

# Brain dead method of retrieving environment var
class Config(BaseSettings):
    BROKER_URL:str = "PLAINTEXT://localhost:9092"
    TOPIC_NAME:str = "my-first-python-topic"

    def getKafkaClientConfig(self):
        return {'bootstrap.servers': self.BROKER_URL}

async def produce(clientConfig:dict,topic_name):
    """Produces data into the Kafka Topic"""
    # TODO: Configure the producer with `bootstrap.servers`
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#producer
    config = Config()
    p = Producer(clientConfig)

    curr_iteration = 0
    while True:
        # TODO: Produce a message to the topic
        #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Producer.produce
        p.produce(topic_name, key=str(curr_iteration),value=f'Message: {curr_iteration}')

        curr_iteration += 1
        await asyncio.sleep(1)


async def consume(clientConfig:dict,topic_name):
    """Consumes data from the Kafka Topic"""
    # TODO: Configure the consumer with `bootstrap.servers` and `group.id`
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#consumer
    clientConfig.update({'group.id':'first-consumer-group'})
    
    c = Consumer(clientConfig)

    # TODO: Subscribe to the topic
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Consumer.subscribe
    c.subscribe([topic_name])

    while True:
        # TODO: Poll for a message
        #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Consumer.poll
        message = c.poll(1.0)

        # TODO: Handle the message. Remember that you should:
        #   1. Check if the message is `None`
        #   2. Check if the message has an error: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Message.error
        #   3. If 1 and 2 were false, print the message key and value
        #       Key: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Message.key
        #       Value: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Message.value
        #
        if message is None:
            print(f"No messages found")
        elif message.error():
            print(f"An error occurred {message.error()}")
        else:
            print(f"Message Key: {message.key()}, Message Value: {message.value()}")

        await asyncio.sleep(1)


async def produce_consume():
    """Runs the Producer and Consumer tasks"""
    config = Config()
    t1 = asyncio.create_task(produce(config.getKafkaClientConfig(), config.TOPIC_NAME))
    t2 = asyncio.create_task(consume(config.getKafkaClientConfig(), config.TOPIC_NAME))
    await t1
    await t2


def main():
    """Runs the exercise"""
    # TODO: Configure the AdminClient with `bootstrap.servers`
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.admin.AdminClient
    config = Config()
    clientConfig = config.getKafkaClientConfig()
    
    client = AdminClient(clientConfig)
    # TODO: Create a NewTopic object. Don't forget to set partitions and replication factor to 1!
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.admin.NewTopic
    topic = NewTopic(config.TOPIC_NAME,num_partitions=1)

    # TODO: Using `client`, create the topic
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.admin.AdminClient.create_topics
    client.create_topics([topic])

    try:
        asyncio.run(produce_consume())
    except KeyboardInterrupt as e:
        print("shutting down")
    finally:
        # TODO: Using `client`, delete the topic
        #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.admin.AdminClient.delete_topics
        client.delete_topics([topic])
        pass


if __name__ == "__main__":
    main()