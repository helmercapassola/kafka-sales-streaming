
from datetime import datetime
from math import isclose, isfinite


# Métodos de pagamento aceites pelo projecto.
PAYMENT_METHODS = {"card", "cash", "transfer"}


def validate_sale_event(sale: dict) -> tuple[bool, str | None]:
    """
    Valida a estrutura e os dados de um evento de venda.

    Retorna:
        (True, None) quando o evento é válido.
        (False, mensagem) quando encontra algum problema.
    """

    # Confirma se o evento recebido é um dicionário.
    if not isinstance(sale, dict):
        return False, "O evento deve ser um dicionário."

    # Verifica a existência dos campos obrigatórios.
    required_fields = {
        "sale_id",
        "product",
        "quantity",
        "unit_price",
        "total_amount",
        "payment_method",
        "timestamp",
    }

    missing_fields = required_fields - sale.keys()

    if missing_fields:
        return False, f"Campos obrigatórios em falta: {', '.join(sorted(missing_fields))}"

    # Valida o identificador da venda.
    if not isinstance(sale["sale_id"], str) or not sale["sale_id"].strip():
        return False, "O identificador da venda é inválido."

    # Valida o nome do produto.
    if not isinstance(sale["product"], str) or not sale["product"].strip():
        return False, "O produto é inválido."

    # Confirma que a quantidade é um número inteiro positivo.
    quantity = sale["quantity"]

    if isinstance(quantity, bool) or not isinstance(quantity, int) or quantity <= 0:
        return False, "A quantidade deve ser um número inteiro positivo."

    # Valida os valores monetários.
    unit_price = sale["unit_price"]
    total_amount = sale["total_amount"]

    if isinstance(unit_price, bool) or not isinstance(unit_price, (int, float)):
        return False, "O preço unitário deve ser numérico."

    if isinstance(total_amount, bool) or not isinstance(total_amount, (int, float)):
        return False, "O valor total deve ser numérico."

    if not isfinite(unit_price) or not isfinite(total_amount):
        return False, "Os valores monetários devem ser finitos."

    if unit_price <= 0 or total_amount <= 0:
        return False, "Os valores monetários devem ser positivos."

    # Confirma se o total corresponde à quantidade vezes o preço unitário.
    expected_total = round(quantity * unit_price, 2)

    if not isclose(total_amount, expected_total, abs_tol=0.01, rel_tol=0):
        return False, "O valor total não corresponde à quantidade vezes o preço unitário."

    # Verifica se o método de pagamento é permitido.
    if sale["payment_method"] not in PAYMENT_METHODS:
        return False, "Método de pagamento não permitido."

    # Confirma se a data está num formato ISO válido.
    if not isinstance(sale["timestamp"], str):
        return False, "A data da venda deve ser uma string."

    try:
        datetime.fromisoformat(sale["timestamp"].replace("Z", "+00:00"))
    except ValueError:
        return False, "A data da venda não está num formato ISO válido."

    return True, None
