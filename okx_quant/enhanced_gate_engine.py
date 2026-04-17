from __future__ import annotations

import ast
from decimal import Decimal
from typing import Any

from okx_quant.enhanced_models import GateCheckResult, GateRuleConfig, QuotaSnapshot


ZERO = Decimal("0")


class EnhancedGateEngine:
    def build_gate_variables(
        self,
        *,
        quota_snapshot: QuotaSnapshot | None = None,
        price: Decimal | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        variables: dict[str, Any] = {
            "price": price if price is not None else ZERO,
        }
        if quota_snapshot is not None:
            variables.update(
                {
                    "long_direction_available": quota_snapshot.available_long_direction,
                    "short_direction_available": quota_snapshot.available_short_direction,
                    "spot_inventory_available": quota_snapshot.available_spot_inventory,
                    "covered_call_available": quota_snapshot.available_covered_call,
                    "cash_secured_put_available": quota_snapshot.available_cash_secured_put,
                    "protected_long_available": quota_snapshot.available_protected_long,
                    "protected_short_available": quota_snapshot.available_protected_short,
                }
            )
        if extra:
            variables.update(extra)
        return variables

    def evaluate_gate(self, gate_rule: GateRuleConfig, variables: dict[str, Any]) -> GateCheckResult:
        matched = bool(_safe_eval(gate_rule.condition_expr, variables))
        if gate_rule.effect == "deny_open":
            allowed = not matched
            reason = "blocked_by_deny_rule" if matched else "deny_rule_not_triggered"
        elif gate_rule.effect == "allow_open":
            allowed = matched
            reason = "allow_rule_matched" if matched else "allow_rule_not_satisfied"
        else:
            allowed = True
            reason = "warn_rule_matched" if matched else "warn_rule_not_matched"
        return GateCheckResult(
            gate_id=gate_rule.gate_id,
            gate_name=gate_rule.gate_name,
            effect=gate_rule.effect,
            matched=matched,
            allowed=allowed,
            reason=reason,
        )

    def evaluate_gates(
        self,
        gate_rules: list[GateRuleConfig],
        variables: dict[str, Any],
    ) -> tuple[bool, str, tuple[GateCheckResult, ...]]:
        results = tuple(self.evaluate_gate(rule, variables) for rule in gate_rules if rule.enabled)
        for result in results:
            if not result.allowed and result.effect == "deny_open":
                return False, f"{result.gate_name}: {result.reason}", results
        for result in results:
            if not result.allowed and result.effect == "allow_open":
                return False, f"{result.gate_name}: {result.reason}", results
        return True, "gates_passed", results


class _SafeEvaluator(ast.NodeVisitor):
    def __init__(self, variables: dict[str, Any]) -> None:
        self._variables = variables

    def visit_Expression(self, node: ast.Expression) -> Any:
        return self.visit(node.body)

    def visit_Name(self, node: ast.Name) -> Any:
        if node.id not in self._variables:
            raise ValueError(f"unknown variable in gate expression: {node.id}")
        return self._variables[node.id]

    def visit_Constant(self, node: ast.Constant) -> Any:
        return node.value

    def visit_BoolOp(self, node: ast.BoolOp) -> Any:
        values = [bool(self.visit(item)) for item in node.values]
        if isinstance(node.op, ast.And):
            return all(values)
        if isinstance(node.op, ast.Or):
            return any(values)
        raise ValueError("unsupported boolean operator")

    def visit_UnaryOp(self, node: ast.UnaryOp) -> Any:
        operand = self.visit(node.operand)
        if isinstance(node.op, ast.Not):
            return not bool(operand)
        if isinstance(node.op, ast.USub):
            return -operand
        if isinstance(node.op, ast.UAdd):
            return +operand
        raise ValueError("unsupported unary operator")

    def visit_BinOp(self, node: ast.BinOp) -> Any:
        left = self.visit(node.left)
        right = self.visit(node.right)
        if isinstance(node.op, ast.Add):
            return left + right
        if isinstance(node.op, ast.Sub):
            return left - right
        if isinstance(node.op, ast.Mult):
            return left * right
        if isinstance(node.op, ast.Div):
            return left / right
        if isinstance(node.op, ast.Mod):
            return left % right
        raise ValueError("unsupported binary operator")

    def visit_Compare(self, node: ast.Compare) -> Any:
        left = self.visit(node.left)
        for operator, comparator in zip(node.ops, node.comparators):
            right = self.visit(comparator)
            if isinstance(operator, ast.Gt):
                matched = left > right
            elif isinstance(operator, ast.GtE):
                matched = left >= right
            elif isinstance(operator, ast.Lt):
                matched = left < right
            elif isinstance(operator, ast.LtE):
                matched = left <= right
            elif isinstance(operator, ast.Eq):
                matched = left == right
            elif isinstance(operator, ast.NotEq):
                matched = left != right
            else:
                raise ValueError("unsupported comparison operator")
            if not matched:
                return False
            left = right
        return True

    def generic_visit(self, node: ast.AST) -> Any:
        raise ValueError(f"unsupported gate expression node: {node.__class__.__name__}")


def _safe_eval(expression: str, variables: dict[str, Any]) -> Any:
    tree = ast.parse(expression, mode="eval")
    evaluator = _SafeEvaluator(variables)
    return evaluator.visit(tree)
