"""Pruebas para biblioteca de funciones en expresiones y variables de entorno."""

import pytest

from flowengine.core.expressions import resolve_expression


def test_environment_variables(monkeypatch):
    monkeypatch.setenv("TEST_API_KEY", "secret-token-12345")
    monkeypatch.setenv("DATABASE_URL", "sqlite:///test.db")

    context = {}
    res_key = resolve_expression("{{env.TEST_API_KEY}}", context)
    assert res_key == "secret-token-12345"

    res_url = resolve_expression("Bearer {{secrets.TEST_API_KEY}}", context)
    assert res_url == "Bearer secret-token-12345"

    res_missing = resolve_expression("{{env.NON_EXISTENT_VAR}}", context)
    assert res_missing is None


def test_string_functions():
    context = {
        "user": {
            "name": "   john doe   ",
            "title": "senior software engineer",
        }
    }

    assert resolve_expression("{{upper(user.name)}}", context) == "   JOHN DOE   "
    assert resolve_expression("{{trim(user.name)}}", context) == "john doe"
    assert resolve_expression("{{upper(trim(user.name))}}", context) == "JOHN DOE"
    assert resolve_expression("{{capitalize(trim(user.name))}}", context) == "John doe"
    assert resolve_expression("{{title(user.title)}}", context) == "Senior Software Engineer"
    assert resolve_expression("{{slugify(user.title)}}", context) == "senior-software-engineer"
    assert (
        resolve_expression("{{replace(user.title, 'senior', 'lead')}}", context) == "lead software engineer"
    )


def test_math_and_arithmetic_functions():
    context = {"total": 125.456, "items": [10, 20, 30], "discount": -15.5}

    assert resolve_expression("{{round(total, 2)}}", context) == 125.46
    assert resolve_expression("{{round(total)}}", context) == 125.0
    assert resolve_expression("{{abs(discount)}}", context) == 15.5
    assert resolve_expression("{{min(10, 5, 20)}}", context) == 5
    assert resolve_expression("{{max(10, 5, 20)}}", context) == 20
    assert resolve_expression("{{sum(items)}}", context) == 60

    # Operación aritmética directa
    assert resolve_expression("{{round(total * 0.19, 2)}}", context) == 23.84
    assert resolve_expression("{{total + 10}}", context) == pytest.approx(135.456)


def test_date_and_json_functions():
    context = {
        "created_at": "2026-10-22T15:30:00Z",
        "payload": {"status": "ok", "count": 2},
    }

    # now()
    now_res = resolve_expression("{{now()}}", context)
    assert isinstance(now_res, str)
    assert "2026" in now_res

    # format_date
    formatted = resolve_expression("{{format_date(created_at, '%Y/%m/%d')}}", context)
    assert formatted == "2026/10/22"

    # json()
    json_str = resolve_expression("{{json(payload)}}", context)
    assert json_str == '{"status": "ok", "count": 2}'
