
# IMPORTAÇÃO DAS BIBLIOTECAS

import json
import logging

# datetime: permite obter a data e hora da venda.
# timezone: permite trabalhar com datas em UTC.
from datetime import datetime, timezone

# uuid4: gera um identificador único para cada venda.
from uuid import uuid4

# Producer: cliente responsável por enviar mensagens ao Kafka.
from confluent_kafka import Producer

# Importamos as configurações centralizadas do projecto.
from kafka_sales.common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_SALES,
)


# CONFIGURAÇÃO DOS LOGS

# Define como as mensagens de execução serão apresentadas.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# Cria um logger para acompanhar a execução do Producer.
logger = logging.getLogger(__name__)


# CALLBACK DE CONFIRMAÇÃO DA ENTREGA

def delivery_report(err, message):
    """
    Esta função é chamada quando o Kafka informa
    o resultado da tentativa de publicação da mensagem.
    """

    # Se existir um erro, significa que a mensagem
    # não foi entregue correctamente.
    if err is not None:
        logger.error("Erro ao publicar venda: %s", err)

    else:
        # Se não existir erro, apresentamos informações
        # sobre onde a mensagem foi armazenada no Kafka.
        logger.info(
            "Venda publicada | Topic: %s | Partition: %s | Offset: %s",
            message.topic(),
            message.partition(),
            message.offset(),
        )


# GERAÇÃO DO EVENTO DE VENDA

def create_sale_event():
    """
    Simula uma venda e devolve os seus dados
    num dicionário Python.
    """

    # Quantidade de produtos vendidos.
    quantity = 2

    # Preço unitário do produto.
    unit_price = 850.00

    # Calcula o valor total da venda.
    total_amount = round(quantity * unit_price, 2)

    # Estrutura dos dados da venda.
    sale_event = {

        # Identificador único da venda.
        "sale_id": str(uuid4()),

        # Nome do produto.
        "product": "Laptop",

        # Quantidade vendida.
        "quantity": quantity,

        # Preço de uma unidade.
        "unit_price": unit_price,

        # Valor total da venda.
        "total_amount": total_amount,

        # Método de pagamento utilizado.
        "payment_method": "card",

        # Data e hora da venda em UTC.
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    return sale_event


#  PRINCIPAL DO PRODUCER

def main():

    # Criamos o cliente Producer.
    # O endereço do Kafka vem do nosso ficheiro config.py.
    producer = Producer({

        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,

        # Identifica o nosso Producer no Kafka.
        "client.id": "sales-producer",
    })

    # Geramos um novo evento de venda.
    sale_event = create_sale_event()

    # Convertemos o dicionário Python para uma string JSON.
    # O Kafka recebe os dados serializados.
    sale_json = json.dumps(sale_event)

    # Publicamos o evento no topic sales.
    producer.produce(

        # Topic onde a mensagem será armazenada.
        topic=KAFKA_TOPIC_SALES,

        # Chave da mensagem.
        # O Kafka utiliza esta chave para determinar
        # a partition de destino.
        key=sale_event["sale_id"],

        # Conteúdo da mensagem em JSON.
        value=sale_json,

        # Função que será chamada para confirmar a entrega.
        callback=delivery_report,
    )

    # Aguarda que as mensagens pendentes sejam processadas
    # e que os callbacks de entrega sejam executados.
    producer.flush()

    logger.info("Processamento do Producer concluído.")


# PONTO DE ENTRADA DA APLICAÇÃO

# Esta condição garante que a função main()
# só é executada quando este ficheiro é chamado directamente.

if __name__ == "__main__":
    main()
