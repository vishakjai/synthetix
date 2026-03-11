from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any


RUN_ID_RE = re.compile(r"\b\d{8}_\d{6}_[a-f0-9]{8}\b", re.IGNORECASE)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _json_block(payload: dict[str, Any]) -> str:
    return json.dumps(payload, indent=2, ensure_ascii=True, default=str)


@dataclass
class EstimationAgent:
    llm: Any | None = None

    def intake(self, *, message: str, mode: str, current: dict[str, Any] | None = None) -> dict[str, Any]:
        current = current or {}
        draft = self._deterministic_intake(message=message, mode=mode, current=current)
        llm_meta = {"used": False, "provider": "", "model": "", "reason": "no_llm"}
        if self.llm is not None:
            enriched, llm_meta = self._llm_intake(message=message, mode=mode, current=current, draft=draft)
            if enriched:
                draft = enriched
        return {"draft": draft, "llm": llm_meta}

    def explain(
        self,
        *,
        estimate_bundle: dict[str, Any],
        question: str,
        wbs_item_id: str = "",
    ) -> dict[str, Any]:
        draft = self._deterministic_explain(estimate_bundle=estimate_bundle, question=question, wbs_item_id=wbs_item_id)
        llm_meta = {"used": False, "provider": "", "model": "", "reason": "no_llm"}
        if self.llm is not None:
            enriched, llm_meta = self._llm_explain(
                estimate_bundle=estimate_bundle,
                question=question,
                wbs_item_id=wbs_item_id,
                draft=draft,
            )
            if enriched:
                draft = enriched
        return {"response": draft, "llm": llm_meta}

    def _deterministic_intake(self, *, message: str, mode: str, current: dict[str, Any]) -> dict[str, Any]:
        text = _clean(message)
        mode_clean = (_clean(mode) or "brownfield").lower()
        run_id = _clean(current.get("run_id"))
        if not run_id:
            match = RUN_ID_RE.search(text)
            if match:
                run_id = match.group(0)
        business_need = _clean(current.get("business_need")) or text
        team_model_key = _clean(current.get("team_model_key")) or "HUMAN_ONLY"
        lower = text.lower()
        if any(word in lower for word in ["agent-assisted", "agent assisted", "hybrid", "hitl"]):
            team_model_key = "HUMAN_LED_AGENT_ASSISTED"
        missing_fields: list[str] = []
        follow_up_questions: list[str] = []
        assumptions: list[str] = []
        recommended_mode = mode_clean
        if mode_clean == "brownfield":
            if not run_id:
                missing_fields.append("run_id")
                follow_up_questions.append("What completed brownfield run ID should the estimate reuse?")
            if not business_need:
                missing_fields.append("business_need")
                follow_up_questions.append("What business outcome should the modernization preserve or improve?")
            assumptions.append("Estimate will use Stage 1 decomposition, risks, and traceability artifacts if a run ID is provided.")
        elif mode_clean == "greenfield":
            if not business_need:
                missing_fields.append("business_need")
                follow_up_questions.append("What user outcome or business capability is the greenfield solution supposed to deliver?")
            follow_up_questions.extend(
                [
                    "What target stack or hosting model is preferred?",
                    "Are there any fixed deadline, compliance, or staffing constraints?",
                ]
            )
            assumptions.append("Confidence stays indicative until target architecture and constraints are confirmed.")
        else:
            recommended_mode = "nl"
            if not business_need:
                missing_fields.append("business_need")
                follow_up_questions.append("What problem are we solving and for whom?")
            follow_up_questions.extend(
                [
                    "Is this a brownfield modernization or a greenfield build?",
                    "Do you already have specs or source artifacts available?",
                ]
            )
            assumptions.append("Estimate cannot be firm until source evidence or structured scope is provided.")
        return {
            "mode": recommended_mode,
            "business_need": business_need,
            "team_model_key": team_model_key,
            "run_id": run_id,
            "missing_fields": missing_fields,
            "follow_up_questions": list(dict.fromkeys([q for q in follow_up_questions if _clean(q)])),
            "assumptions": assumptions,
            "confidence_tier": "PLANNING" if recommended_mode == "brownfield" and run_id else "INDICATIVE",
        }

    def _deterministic_explain(
        self,
        *,
        estimate_bundle: dict[str, Any],
        question: str,
        wbs_item_id: str = "",
    ) -> dict[str, Any]:
        summary = estimate_bundle.get("estimate_summary") or {}
        ledger = estimate_bundle.get("assumption_ledger") or {}
        wbs = estimate_bundle.get("wbs") or {}
        estimate = summary.get("estimate") if isinstance(summary.get("estimate"), dict) else {}
        items = (
            wbs.get("wbs", {}).get("items", [])
            if isinstance(wbs.get("wbs", {}), dict)
            else []
        )
        selected = None
        if wbs_item_id:
            for item in items:
                if isinstance(item, dict) and _clean(item.get("wbs_item_id")) == _clean(wbs_item_id):
                    selected = item
                    break
        if selected is None and items:
            selected = items[0] if isinstance(items[0], dict) else None
        assumptions = ledger.get("assumptions", []) if isinstance(ledger.get("assumptions", []), list) else []
        cited_assumptions = [row.get("id") for row in assumptions[:4] if isinstance(row, dict) and _clean(row.get("id"))]
        answer = (
            f"Estimate { _clean(summary.get('meta', {}).get('artifact_id')) or 'summary' } "
            f"uses the deterministic brownfield kernel. Total likely effort is "
            f"{estimate.get('effort', {}).get('hours', {}).get('p50', 'n/a')} hours over "
            f"{estimate.get('timeline', {}).get('weeks', {}).get('p50', 'n/a')} weeks under "
            f"{_clean(estimate.get('team_model', {}).get('key')) or 'the selected team model'}."
        )
        if selected:
            answer += (
                f" Focus item { _clean(selected.get('wbs_item_id')) }: "
                f"{_clean(selected.get('title'))} is sized { _clean(selected.get('size_tier')) or 'n/a' } "
                f"at {selected.get('effort_hours', {}).get('p50', 'n/a')} likely hours."
            )
        if cited_assumptions:
            answer += f" Referenced assumptions: {', '.join(cited_assumptions)}."
        return {
            "answer": answer,
            "question": _clean(question),
            "wbs_item_id": _clean(wbs_item_id) or _clean(selected.get("wbs_item_id") if isinstance(selected, dict) else ""),
            "assumption_refs": cited_assumptions,
            "mode": "deterministic",
        }

    def _llm_intake(
        self,
        *,
        message: str,
        mode: str,
        current: dict[str, Any],
        draft: dict[str, Any],
    ) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        system_prompt = (
            "You are the Synthetix Estimation Agent intake shell. "
            "Do not estimate hours or timelines. "
            "Convert the user's message into structured intake fields, explicit missing information, "
            "and concise follow-up questions. Return JSON only."
        )
        user_prompt = (
            f"Current mode: {mode}\n"
            f"Current draft:\n{_json_block(draft)}\n"
            f"Current UI state:\n{_json_block(current)}\n"
            f"User message:\n{message}\n"
            "Return JSON with keys: mode, business_need, team_model_key, run_id, "
            "missing_fields, follow_up_questions, assumptions, confidence_tier."
        )
        try:
            response = self.llm.invoke(system_prompt=system_prompt, user_message=user_prompt)
        except Exception as exc:
            return None, {"used": False, "provider": "", "model": "", "reason": f"llm_invoke_failed:{exc}"}
        content = _clean(getattr(response, "content", ""))
        try:
            parsed = json.loads(content)
        except Exception:
            return None, {"used": False, "provider": str(getattr(response, "provider", "")), "model": str(getattr(response, "model", "")), "reason": "invalid_json"}
        if not isinstance(parsed, dict):
            return None, {"used": False, "provider": str(getattr(response, "provider", "")), "model": str(getattr(response, "model", "")), "reason": "invalid_shape"}
        merged = dict(draft)
        merged.update({k: v for k, v in parsed.items() if k in {"mode", "business_need", "team_model_key", "run_id", "confidence_tier"}})
        for key in ("missing_fields", "follow_up_questions", "assumptions"):
            if isinstance(parsed.get(key), list):
                merged[key] = parsed[key]
        return merged, {
            "used": True,
            "provider": str(getattr(response, "provider", "")),
            "model": str(getattr(response, "model", "")),
            "reason": "",
        }

    def _llm_explain(
        self,
        *,
        estimate_bundle: dict[str, Any],
        question: str,
        wbs_item_id: str,
        draft: dict[str, Any],
    ) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        system_prompt = (
            "You are the Synthetix Estimation Agent explanation shell. "
            "Explain the already-computed deterministic estimate. "
            "Do not invent new numbers or alter the estimate. Return JSON only."
        )
        user_prompt = (
            f"Question: {question}\n"
            f"WBS item: {wbs_item_id or 'none'}\n"
            f"Deterministic draft answer:\n{_json_block(draft)}\n"
            f"Estimate summary:\n{_json_block(estimate_bundle.get('estimate_summary') or {})}\n"
            f"Assumption ledger:\n{_json_block(estimate_bundle.get('assumption_ledger') or {})}\n"
            f"WBS:\n{_json_block(estimate_bundle.get('wbs') or {})}\n"
            "Return JSON with keys: answer, question, wbs_item_id, assumption_refs, mode."
        )
        try:
            response = self.llm.invoke(system_prompt=system_prompt, user_message=user_prompt)
        except Exception as exc:
            return None, {"used": False, "provider": "", "model": "", "reason": f"llm_invoke_failed:{exc}"}
        content = _clean(getattr(response, "content", ""))
        try:
            parsed = json.loads(content)
        except Exception:
            return None, {"used": False, "provider": str(getattr(response, "provider", "")), "model": str(getattr(response, "model", "")), "reason": "invalid_json"}
        if not isinstance(parsed, dict):
            return None, {"used": False, "provider": str(getattr(response, "provider", "")), "model": str(getattr(response, "model", "")), "reason": "invalid_shape"}
        merged = dict(draft)
        merged.update({k: v for k, v in parsed.items() if k in {"answer", "question", "wbs_item_id", "mode"}})
        if isinstance(parsed.get("assumption_refs"), list):
            merged["assumption_refs"] = parsed["assumption_refs"]
        return merged, {
            "used": True,
            "provider": str(getattr(response, "provider", "")),
            "model": str(getattr(response, "model", "")),
            "reason": "",
        }
