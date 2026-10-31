
import unittest

from kafka_sales.common.validators import validate_sale_event


class TestSaleValidation(unittest.TestCase):

    def setUp(self):
        # Cria um evento de venda válido para reutilizar nos testes.
        self.valid_sale = {
            "sale_id": "sale-001",
            "product": "Laptop",
            "quantity": 2,
            "unit_price": 850.00,
            "total_amount": 1700.00,
            "payment_method": "card",
            "timestamp": "2026-10-31T14:00:00+00:00",
        }

    def test_valid_sale(self):
        # Um evento correcto deve ser aceite.
        is_valid, error = validate_sale_event(self.valid_sale)

        self.assertTrue(is_valid)
        self.assertIsNone(error)

    def test_missing_required_field(self):
        # Um evento sem identificador deve ser rejeitado.
        sale = self.valid_sale.copy()
        del sale["sale_id"]

        is_valid, error = validate_sale_event(sale)

        self.assertFalse(is_valid)
        self.assertIn("Campos obrigatórios em falta", error)

    def test_invalid_quantity(self):
        # A quantidade não pode ser zero.
        sale = self.valid_sale.copy()
        sale["quantity"] = 0

        is_valid, error = validate_sale_event(sale)

        self.assertFalse(is_valid)
        self.assertIn("quantidade", error)

    def test_incorrect_total_amount(self):
        # O total deve corresponder à quantidade multiplicada pelo preço.
        sale = self.valid_sale.copy()
        sale["total_amount"] = 1500.00

        is_valid, error = validate_sale_event(sale)

        self.assertFalse(is_valid)
        self.assertIn("valor total", error)

    def test_invalid_payment_method(self):
        # Apenas os métodos de pagamento definidos são aceites.
        sale = self.valid_sale.copy()
        sale["payment_method"] = "bitcoin"

        is_valid, error = validate_sale_event(sale)

        self.assertFalse(is_valid)
        self.assertIn("Método de pagamento", error)

    def test_invalid_timestamp(self):
        # A data deve respeitar o formato ISO.
        sale = self.valid_sale.copy()
        sale["timestamp"] = "data-invalida"

        is_valid, error = validate_sale_event(sale)

        self.assertFalse(is_valid)
        self.assertIn("data da venda", error)


if __name__ == "__main__":
    unittest.main()
