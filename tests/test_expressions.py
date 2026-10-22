"""Pruebas unitarias para el evaluador de expresiones de FlowEngine."""

from flowengine.core.expressions import (
    SafeConditionEvaluator,
    get_nested_value,
    resolve_expression,
)


def test_get_nested_value():
    data = {
        "user": {
            "name": "Alice",
            "roles": ["admin", "editor"],
            "address": {"city": "Pereira"},
        },
        "count": 42,
    }

    assert get_nested_value(data, "user.name") == "Alice"
    assert get_nested_value(data, "user.roles.0") == "admin"
    assert get_nested_value(data, "user.roles[1]") == "editor"
    assert get_nested_value(data, "user.address.city") == "Pereira"
    assert get_nested_value(data, "count") == 42
    assert get_nested_value(data, "user.non_existent") is None
    assert get_nested_value(None, "foo.bar") is None


def test_resolve_expression_exact_type():
    context = {"total": 150, "items": ["a", "b"], "active": True}

    assert resolve_expression("{{total}}", context) == 150
    assert resolve_expression("{{items}}", context) == ["a", "b"]
    assert resolve_expression("{{active}}", context) is True


def test_resolve_expression_string_interpolation():
    context = {
        "user": {"name": "Carlos"},
        "order": {"id": 99},
    }
    template = "Hola {{user.name}}, su pedido #{{order.id}} fue procesado."
    res = resolve_expression(template, context)
    assert res == "Hola Carlos, su pedido #99 fue procesado."


def test_resolve_expression_nested_dict_and_list():
    context = {"status": "ok", "code": 200}
    template = {
        "header": "Status is {{status}}",
        "nested": {"status_code": "{{code}}"},
        "list": ["Code: {{code}}", "{{status}}"],
    }
    res = resolve_expression(template, context)
    assert res == {
        "header": "Status is ok",
        "nested": {"status_code": 200},
        "list": ["Code: 200", "ok"],
    }


def test_condition_evaluator_comparisons():
    context = {
        "status": 200,
        "total": 150.5,
        "country": "CO",
        "email": "user@example.com",
        "tags": ["vip", "promo"],
        "empty_list": [],
    }

    assert SafeConditionEvaluator.evaluate("{{status}} == 200", context) is True
    assert SafeConditionEvaluator.evaluate("{{status}} != 404", context) is True
    assert SafeConditionEvaluator.evaluate("{{total}} > 100", context) is True
    assert SafeConditionEvaluator.evaluate("{{total}} <= 200", context) is True
    assert SafeConditionEvaluator.evaluate("{{country}} == 'CO' and {{status}} == 200", context) is True
    assert SafeConditionEvaluator.evaluate("{{country}} == 'US' or {{total}} > 100", context) is True
    assert SafeConditionEvaluator.evaluate("{{email}} contains '@example.com'", context) is True
    assert SafeConditionEvaluator.evaluate("'vip' in {{tags}}", context) is True
    assert SafeConditionEvaluator.evaluate("{{empty_list}} is_empty", context) is True


def test_condition_evaluator_safety():
    # Asegurar que código arbitrario peligroso no se ejecute
    context = {}
    assert SafeConditionEvaluator.evaluate("__import__('os').system('dir')", context) is False
