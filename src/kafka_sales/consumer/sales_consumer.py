
import json
import logging

from confluent_kafka import Consumer

from kafka_sales.common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_SALES,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def main():

    # Configura a ligação ao Kafka e identifica o grupo de consumidores.
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "sales-consumer-group",

        # Permite ler mensagens antigas quando o grupo ainda não tem offset.
        "auto.offset.reset": "earliest",

        # Só confirma a leitura depois de processarmos a mensagem.
        "enable.auto.commit": False,
    })

    # Inscreve o Consumer no topic de vendas.
    consumer.subscribe([KAFKA_TOPIC_SALES])

    logger.info("Consumer iniciado. À espera de vendas...")

    try:
        while True:

            # Aguarda uma mensagem durante, no máximo, um segundo.
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                logger.error("Erro ao receber mensagem: %s", message.error())
                continue

            # Converte os bytes recebidos para texto e depois para dicionário.
            sale = json.loads(message.value().decode("utf-8"))

            logger.info(
                "Venda recebida | ID: %s | Produto: %s | Total: %.2f",
                sale["sale_id"],
                sale["product"],
                sale["total_amount"],
            )

            # Confirma a leitura apenas depois do processamento bem-sucedido.
            consumer.commit(message=message, asynchronous=False)

    except KeyboardInterrupt:
        logger.info("Interrupção solicitada pelo utilizador.")

    finally:
        # Fecha a ligação e liberta os recursos do Consumer.
        consumer.close()
        logger.info("Consumer encerrado.")


if __name__ == "__main__":
    main()
