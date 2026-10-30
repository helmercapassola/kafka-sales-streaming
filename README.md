# 🚀 Real-Time Sales Streaming Pipeline

<div align="center">

![Status](https://img.shields.io/badge/Status-In%20Development-FFA500?style=for-the-badge)
![Portfolio](https://img.shields.io/badge/Category-Data%20Engineering-7C5CFF?style=for-the-badge)
![Kafka](https://img.shields.io/badge/Streaming-Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)

</div>

> Data Engineering Portfolio Project

Uma solução de streaming de eventos em tempo real para simular, processar e armazenar vendas online através de Kafka, Python e PostgreSQL.

## Visão geral

Este projecto demonstra uma arquitetura event-driven em que os eventos de venda são gerados em Python, publicados num topic do Kafka, validados e processados por um consumidor e armazenados numa base de dados relacional para análise posterior.

O objetivo principal é praticar os conceitos fundamentais de engenharia de dados e streaming: produção de eventos, distribuição em tópicos, processamento paralelo, persistência e observabilidade.

### Highlights do projecto

- 🔁 Pipeline de dados em tempo real
- 🧠 Simulação de eventos de vendas em Python
- ⚡ Publicação e consumo com Kafka
- 🗄️ Armazenamento em PostgreSQL
- 📊 Prática com análise e monitorização de dados

## Objetivos do projecto

- Desenvolver um sistema de processamento de eventos em tempo real.
- Implementar producers e consumers em Python.
- Compreender a arquitetura e o funcionamento do Apache Kafka.
- Trabalhar com topics e partitions.
- Integrar Kafka com PostgreSQL.
- Validar e tratar erros nos eventos recebidos.
- Utilizar Docker para orquestrar a infraestrutura local.
- Realizar consultas SQL para análise dos dados de vendas.
- Documentar e versionar o projecto com Git e GitHub.

## 🏗️ Arquitectura da solução

A imagem abaixo representa o fluxo completo dos dados, desde a geração dos eventos até à análise final.

![Arquitetura da solução](./assets/architecture.svg)

### Fluxo da solução

- Simulação de vendas em Python.
- Publicação de eventos no topic `sales`.
- Distribuição via partitions do Kafka.
- Consumo, validação e processamento dos dados.
- Persistência em PostgreSQL.
- Monitorização com Kafka UI e análise SQL.

## ⚙️ Stack tecnológica

<div align="left">

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![GitHub](https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white)
![DBeaver](https://img.shields.io/badge/DBeaver-59666C?style=for-the-badge&logo=dbeaver&logoColor=white)

</div>

| Tecnologia | Responsabilidade |
| --- | --- |
| Python | Produção e consumo de eventos |
| Apache Kafka | Streaming e distribuição de eventos |
| Kafka UI | Monitorização e observabilidade do cluster |
| PostgreSQL | Armazenamento dos dados processados |
| Docker | Execução dos serviços locais |
| Docker Compose | Orquestração dos containers |
| Git / GitHub | Versionamento e documentação |
| DBeaver | Consulta e análise dos dados |

Tecnologias previstas para evolução:

- Apache Airflow — orquestração de workflows e pipelines batch.

## 🐳 Infraestrutura Docker

A infraestrutura é gerida com Docker Compose e inclui os principais serviços do pipeline.

| Serviço | Container | Porta |
| --- | --- | --- |
| Apache Kafka | `kafka-sales` | `9092` |
| PostgreSQL | `postgres-sales` | `5432` |
| Kafka UI | `kafka-ui` | `8080` |

O ambiente utiliza Kafka em modo KRaft, dispensando a necessidade de ZooKeeper.

### Aceder aos serviços

- Kafka: `localhost:9092`
- PostgreSQL: `localhost:5432`
- Kafka UI: `http://localhost:8080`

## 📨 Configuração do Kafka

O primeiro topic do projecto será:

| Propriedade | Configuração |
| --- | --- |
| Topic | `sales` |
| Partitions | `3` |
| Replication Factor | `1` |
| Cleanup Policy | `delete` |

As três partitions permitem explorar a distribuição dos eventos e o paralelismo do consumo. Como o ambiente utiliza apenas um broker, o replication factor será 1.

## 🧾 Exemplo de evento de venda

Os eventos são transmitidos em formato JSON.

```json
{
  "sale_id": 1001,
  "product": "Laptop",
  "quantity": 2,
  "price": 850.00,
  "timestamp": "2026-10-30T15:30:00"
}
```

Cada evento representa uma venda realizada numa loja online. O producer é responsável por gerar e publicar os dados no topic `sales`, enquanto o consumer recebe, valida e processa as mensagens antes de as persistir no PostgreSQL.

## 📁 Estrutura do projecto

```text
real-time-sales-streaming/
├── assets/
│   └── architecture.svg
├── config/
│   └── settings.py
├── producer/
│   └── sales_producer.py
├── consumer/
│   └── sales_consumer.py
├── database/
│   └── init.sql
├── docker/
│   └── docker-compose.yml
├── logs/
├── .env.example
├── .gitignore
├── requirements.txt
├── README.md
└── tests/
```

A estrutura pode sofrer ajustes conforme a implementação evolui.

## 🗺️ Plano de desenvolvimento

| Etapa | Actividade | Estado |
| --- | --- | --- |
| 1 | Estrutura do projecto | Concluída |
| 2 | Configuração da infraestrutura Docker | Concluída |
| 3 | Criação do topic `sales` | Próxima etapa |
| 4 | Desenvolvimento do producer em Python | Pendente |
| 5 | Desenvolvimento do consumer em Python | Pendente |
| 6 | Integração com PostgreSQL | Pendente |
| 7 | Validação e tratamento de erros | Pendente |
| 8 | Testes e monitorização | Pendente |
| 9 | Documentação e publicação no GitHub | Pendente |

## ▶️ Como executar o projecto

### Pré-requisitos

- Windows
- Python 3
- Docker Desktop
- Docker Compose
- Git

### Iniciar a infraestrutura

Na raiz do projecto, execute:

```powershell
docker compose up -d
```

### Verificar os containers

```powershell
docker compose ps
```

### Consultar os logs

```powershell
docker compose logs -f
```

### Aceder ao Kafka UI

Abra no browser:

```text
http://localhost:8080
```

### Parar os serviços

```powershell
docker compose down
```

Os dados persistentes são mantidos nos volumes Docker, salvo se forem removidos explicitamente.

## 🎯 Resultados esperados

No final do projecto, pretende-se ter uma solução funcional capaz de:

- Simular vendas através de eventos gerados em Python.
- Publicar eventos no Apache Kafka.
- Distribuir eventos através de um topic com três partitions.
- Consumir e processar evento em Python.
- Validar os dados recebidos.
- Armazenar as vendas no PostgreSQL.
- Consultar e analisar os dados via SQL.
- Monitorizar o fluxo de eventos no Kafka UI.

## 🧠 Competências desenvolvidas

Este projecto permite consolidar competências práticas em:

- Data Engineering
- Event-Driven Architecture
- Real-Time Data Streaming
- Apache Kafka
- Python Programming
- PostgreSQL e SQL
- Docker e Docker Compose
- Data Validation
- Git e GitHub
- Data Pipeline Development

## 🚀 Evolução futura

Numa fase posterior, pretende-se integrar Apache Airflow para explorar a orquestração de processos de dados, nomeadamente tarefas batch e a coordenação de pipelines.

Outras melhorias possíveis incluem:

- Implementação de logging estruturado.
- Tratamento de mensagens inválidas.
- Dead Letter Queue (DLQ).
- Monitorização de métricas.
- Testes automatizados.
- Dashboard de indicadores de vendas.

---

Projecto: Real-Time Sales Streaming Pipeline  
Área: Data Engineering  
Tipo: Portfolio Project  
Estado: Em desenvolvimento

Desenvolvido como projecto prático de aprendizagem e demonstração de competências em Engenharia de Dados.