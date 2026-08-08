from __future__ import annotations

import asyncio

import structlog
from langgraph.graph import END, START, StateGraph

from besm.config.schema import LLMBackendConfig
from besm.engine.fallback_queue import RedisFallbackQueue
from besm.engine.models import ScoredSignal
from besm.reasoning.agents.domains import (
    CommodityAgent,
    FinancialAgent,
    HealthAgent,
    LegalAgent,
    MarketAgent,
    UrbanAgent,
)
from besm.reasoning.aggregator import aggregator_node
from besm.reasoning.state import AlertPayload, ReasoningState
from besm.reasoning.supervisor import route_after_supervisor, supervisor_node

log = structlog.get_logger()

_AGENT_TIMEOUT_S = 30
_FALLBACK_RETRY_S = 300


def build_graph(llm: LLMBackendConfig) -> StateGraph:
    commodity = CommodityAgent(llm)
    financial = FinancialAgent(llm)
    legal = LegalAgent(llm)
    health = HealthAgent(llm)
    urban = UrbanAgent(llm)
    market = MarketAgent(llm)

    async def commodity_node(s: ReasoningState) -> ReasoningState:
        return await _run_agent(commodity, s)

    async def financial_node(s: ReasoningState) -> ReasoningState:
        return await _run_agent(financial, s)

    async def legal_node(s: ReasoningState) -> ReasoningState:
        return await _run_agent(legal, s)

    async def health_node(s: ReasoningState) -> ReasoningState:
        return await _run_agent(health, s)

    async def urban_node(s: ReasoningState) -> ReasoningState:
        return await _run_agent(urban, s)

    async def market_node(s: ReasoningState) -> ReasoningState:
        return await _run_agent(market, s)

    async def multi_node(s: ReasoningState) -> ReasoningState:
        results = await asyncio.gather(
            _run_agent(commodity, s),
            _run_agent(financial, s),
            _run_agent(legal, s),
            _run_agent(health, s),
            _run_agent(urban, s),
            _run_agent(market, s),
            return_exceptions=True,
        )
        merged_outputs = []
        for r in results:
            if isinstance(r, Exception):
                continue
            merged_outputs.extend(r["agent_outputs"])
        return {**s, "agent_outputs": merged_outputs}

    graph = StateGraph(ReasoningState)
    graph.add_node("supervisor", supervisor_node)
    graph.add_node("commodity", commodity_node)
    graph.add_node("financial", financial_node)
    graph.add_node("legal", legal_node)
    graph.add_node("health", health_node)
    graph.add_node("urban", urban_node)
    graph.add_node("market", market_node)
    graph.add_node("multi", multi_node)
    graph.add_node("aggregator", aggregator_node)

    graph.add_edge(START, "supervisor")
    graph.add_conditional_edges(
        "supervisor",
        route_after_supervisor,
        {
            "commodity": "commodity",
            "financial": "financial",
            "legal": "legal",
            "health": "health",
            "urban": "urban",
            "market": "market",
            "multi": "multi",
        },
    )
    for domain in (
        "commodity",
        "financial",
        "legal",
        "health",
        "urban",
        "market",
        "multi",
    ):
        graph.add_edge(domain, "aggregator")
    graph.add_edge("aggregator", END)

    return graph.compile()


async def _run_agent(agent, state: ReasoningState) -> ReasoningState:
    try:
        async with asyncio.timeout(_AGENT_TIMEOUT_S):
            return await agent.validate(state)
    except TimeoutError:
        log.error(
            "agent_timeout",
            agent=agent.domain,
            signal_id=state["signal"].signal_id,
        )
        raise


async def process_signal(
    signal: ScoredSignal,
    llm: LLMBackendConfig,
    fallback_queue: RedisFallbackQueue,
) -> AlertPayload | None:
    graph = build_graph(llm)
    initial: ReasoningState = {
        "signal": signal,
        "domain": signal.domain,
        "confidence_override": None,
        "agent_outputs": [],
        "final_confidence": 0.0,
        "alert_approved": False,
        "alert_payload": None,
    }
    try:
        result: ReasoningState = await graph.ainvoke(initial)
    except Exception as exc:
        log.error("graph_error", signal_id=signal.signal_id, error=str(exc))
        await fallback_queue.push(signal)
        return None

    return result["alert_payload"]
