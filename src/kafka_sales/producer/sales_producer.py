
# IMPORTAÇÃO DAS BIBLIOTECAS

import json
import logging
import random
import time
from datetime import datetime, timezone
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

# Produtos disponíveis e respectivos preços unitários.
PRODUCTS = {
    "Laptop": 850.00,
    "Smartphone": 400.00,
    "Tablet": 300.00,
    "Headphones": 100.00,
    "Smartwatch": 200.00,
    "Camera": 500.00,
    "Monitor": 250.00,
}

# Metodos de pagamento disponíveis.
PAYMENT_METHODS = ["card", "cash", "transfer"]


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
    # Seleciona um produto aleatório da lista de produtos disponíveis.
    product = random.choice(list(PRODUCTS.keys()))
    quantity = random.randint(1, 5)  # Quantidade aleatória entre 1 e 5.
    unit_price = PRODUCTS[product]  # Preço unitário do produto selecionado.
    
    # Calcula o valor total da venda.
    total_amount = round(quantity * unit_price, 2)

  
    # Constrói o evento de que sera enviado para o Kafka.
   
    return {
        "sale_id": str(uuid4()),  # Identificador único da venda.
        "product": product,  # Nome do produto vendido.
        "quantity": quantity,  # Quantidade vendida.
        "unit_price": unit_price,  # Preço unitário do produto.
        "total_amount": total_amount,  # Valor total da venda.
        "payment_method": random.choice(PAYMENT_METHODS),  # Método de pagamento aleatório.
        "timestamp": datetime.now(timezone.utc).isoformat(),  # Data e hora da venda em UTC.
    }


#  PRINCIPAL DO PRODUCER

def main():

    # Criamos o cliente Producer.
    # O endereço do Kafka vem do nosso ficheiro config.py.
    producer = Producer({

        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        # Identifica o nosso Producer no Kafka.
        "client.id": "sales-producer",
    })
    
    logger.info("Iniciando o Producer e gerar vendas...")

    try:
        # Continua a gerar e publicar vendar em quanto o porgrama estiver em execução.
        while True:
            sale_event = create_sale_event()  # Gera um evento de venda.
            
            # Envia o evento para o topic sales
            producer.produce(
                topic=KAFKA_TOPIC_SALES,
                key=sale_event["sale_id"],  # Chave da mensagem (ID da venda).
                value=json.dumps(sale_event),  # Valor da mensagem (evento de venda em JSON).
                callback=delivery_report,  # Função de callback para confirmação de entrega.
            )
            
            # processa o callback de entrega das mensagens.
            producer.poll(0)
            
            # Aguarda dois segundos antes de gerar a próxima venda.
            time.sleep(2)
            
    except KeyboardInterrupt:
        logger.info("Interrupção do programa pelo utilizador.")
    finally:
        logger.info("Fechando o Producer...")
        producer.flush()  # Aguarda que todas as mensagens pendentes sejam processadas.
        


if __name__ == "__main__":
    main()
