
import json
import logging

from confluent_kafka import Consumer

from kafka_sales.common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_SALES,
)

from kafka_sales.common.validators import validate_sale_event


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

        # O offset só é confirmado manualmente depois do processamento.
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

            try:
                # Converte a mensagem recebida para um dicionário Python.
                sale = json.loads(message.value().decode("utf-8"))

            except (json.JSONDecodeError, UnicodeDecodeError) as error:
                logger.error(
                    "Mensagem rejeitada: conteúdo JSON inválido. Erro: %s",
                    error,
                )

                # Confirma a mensagem inválida para não a processar repetidamente.
                consumer.commit(message=message, asynchronous=False)
                continue

            # Valida os campos e os valores do evento de venda.
            is_valid, error_message = validate_sale_event(sale)

            if not is_valid:
                logger.warning(
                    "Venda rejeitada | Motivo: %s | Partition: %s | Offset: %s",
                    error_message,
                    message.partition(),
                    message.offset(),
                )

                # Por enquanto, descartamos eventos inválidos após os registar.
                consumer.commit(message=message, asynchronous=False)
                continue

            # Regista a venda depois de confirmar que os dados são válidos.
            logger.info(
                "Venda validada | ID: %s | Produto: %s | Total: %.2f",
                sale["sale_id"],
                sale["product"],
                sale["total_amount"],
            )

            # Confirma a leitura depois de concluir o processamento.
            consumer.commit(message=message, asynchronous=False)

    except KeyboardInterrupt:
        logger.info("Interrupção solicitada pelo utilizador.")

    finally:
        # Fecha a ligação e liberta os recursos do Consumer.
        consumer.close()
        logger.info("Consumer encerrado.")


if __name__ == "__main__":
    main()
