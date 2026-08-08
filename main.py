from __future__ import annotations

import asyncio
import os
import time

import structlog
from redis.asyncio import Redis

from besm.config.loader import LiveConfig
from besm.config.secrets import load_secrets
from besm.engine.consumer import ButterflyEngineConsumer
from besm.engine.fallback_queue import RedisFallbackQueue
from besm.engine.models import ScoredSignal
from besm.gateway.dispatcher import Dispatcher
from besm.gateway.rate_limiter import RateLimiter
from besm.ingestion.nodes.commodity.imd_rainfall_node import IMDRainfallNode
from besm.ingestion.nodes.commodity.import_duty_node import ImportDutyNode
from besm.ingestion.nodes.commodity.mandi_node import MandiNode
from besm.ingestion.nodes.commodity.mrp_monitor_node import MRPMonitorNode
from besm.ingestion.nodes.financial.brent_crude_node import BrentCrudeNode
from besm.ingestion.nodes.financial.forex_node import ForexNode
from besm.ingestion.nodes.financial.gmp_node import GMPNode
from besm.ingestion.nodes.financial.gold_sentiment_node import GoldSentimentNode
from besm.ingestion.nodes.financial.ipo_subscription_node import IPOSubscriptionNode
from besm.ingestion.nodes.financial.panchang_node import PanchangNode
from besm.ingestion.nodes.health.aqi_node import AQINode
from besm.ingestion.nodes.health.imd_weather_node import IMDWeatherNode
from besm.ingestion.nodes.health.social_sentiment_node import SocialSentimentNode
from besm.ingestion.nodes.legal.flight_status_node import FlightStatusNode
from besm.ingestion.nodes.legal.gazette_node import GazetteNode
from besm.ingestion.nodes.legal.power_outage_node import PowerOutageNode
from besm.ingestion.nodes.legal.slot_sentinel_node import SlotSentinelNode
from besm.ingestion.nodes.legal.state_portal_node import StatePortalNode
from besm.ingestion.nodes.market.business_directory_node import BusinessDirectoryNode
from besm.ingestion.nodes.market.infrastructure_node import InfrastructureNode
from besm.ingestion.nodes.market.job_board_node import JobBoardNode
from besm.ingestion.nodes.market.municipal_gazette_node import MunicipalGazetteNode
from besm.ingestion.nodes.urban.social_geotagged_node import SocialGeotaggedNode
from besm.ingestion.nodes.urban.traffic_node import TrafficNode
from besm.ingestion.scheduler import IngestionScheduler
from besm.observability.health import DeliveryMonitor, run_startup_checks
from besm.observability.logger import configure_logging
from besm.observability.metrics import MetricsCollector
from besm.reasoning.graph import process_signal
from besm.vector_store.client import get_client
from besm.vector_store.collections import ensure_collections
from besm.vector_store.retention import run_retention_cleanup
from besm.vector_store.store import VectorStore

log = structlog.get_logger()


async def main() -> None:
    load_secrets()
    live_config = LiveConfig()
    config = live_config.current

    configure_logging(config.logging.level, config.logging.log_file)
    metrics = MetricsCollector(config.logging.metrics_file)
    delivery_monitor = DeliveryMonitor()

    redis = Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"))
    config_path = os.getenv("BESM_CONFIG_PATH", "config.yaml")
    await run_startup_checks(redis, config_path)

    qdrant = get_client(config.vector_store.path)
    ensure_collections(qdrant)
    vector_store = VectorStore(qdrant)

    rate_limiter = RateLimiter(redis, live_config)
    dispatcher = Dispatcher(config, rate_limiter)

    scored_queue: asyncio.Queue[ScoredSignal] = asyncio.Queue()
    fallback_queue = RedisFallbackQueue(redis)

    engine_consumer = ButterflyEngineConsumer(
        redis=redis,
        vector_store=vector_store,
        config=config,
        on_scored=scored_queue,
    )

    scheduler = IngestionScheduler(live_config)
    llm = config.llm
    n = config.nodes

    if n.commodity_mandi:         scheduler.register(MandiNode(redis, llm))
    if n.commodity_imd_rainfall:  scheduler.register(IMDRainfallNode(redis, llm))
    if n.commodity_import_duty:   scheduler.register(ImportDutyNode(redis, llm))
    if n.commodity_mrp_monitor:   scheduler.register(MRPMonitorNode(redis, llm))

    if n.financial_forex:            scheduler.register(ForexNode(redis, llm))
    if n.financial_brent_crude:      scheduler.register(BrentCrudeNode(redis, llm))
    if n.financial_gmp:              scheduler.register(GMPNode(redis, llm))
    if n.financial_ipo_subscription: scheduler.register(IPOSubscriptionNode(redis, llm))
    if n.financial_gold_sentiment:   scheduler.register(GoldSentimentNode(redis, llm))
    if n.financial_panchang:         scheduler.register(PanchangNode(redis, llm))

    if n.legal_gazette:       scheduler.register(GazetteNode(redis, llm))
    if n.legal_state_portal:  scheduler.register(StatePortalNode(redis, llm))
    if n.legal_slot_sentinel: scheduler.register(SlotSentinelNode(redis, llm))
    if n.legal_power_outage:  scheduler.register(PowerOutageNode(redis, llm))
    if n.legal_flight_status and config.financial.monitored_flights:
        scheduler.register(FlightStatusNode(redis, llm))

    if n.health_aqi:              scheduler.register(AQINode(redis, llm))
    if n.health_imd_weather:      scheduler.register(IMDWeatherNode(redis, llm))
    if n.health_social_sentiment: scheduler.register(SocialSentimentNode(redis, llm))

    if n.urban_social_geotagged: scheduler.register(SocialGeotaggedNode(redis, llm))
    if n.urban_traffic:          scheduler.register(TrafficNode(redis, llm))

    if n.market_job_board:           scheduler.register(JobBoardNode(redis, llm))
    if n.market_municipal_gazette:   scheduler.register(MunicipalGazetteNode(redis, llm))
    if n.market_infrastructure:      scheduler.register(InfrastructureNode(redis, llm))
    if n.market_business_directory:  scheduler.register(BusinessDirectoryNode(redis, llm))

    scheduler.start()
    log.info("besm_started")

    from besm.engine.impact_confirmer import ImpactConfirmer

    impact_confirmer = ImpactConfirmer(
        vector_store,
        live_config,
        check_interval_hours=config.system.impact_check_interval_hours,
        confirmation_window_hours=config.system.impact_confirmation_window_hours,
    )

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    loop.add_signal_handler(__import__("signal").SIGINT, lambda: stop_event.set())
    loop.add_signal_handler(__import__("signal").SIGTERM, lambda: stop_event.set())

    tasks = [
        asyncio.create_task(engine_consumer.start(), name="engine"),
        asyncio.create_task(
            _reasoning_loop(
                scored_queue,
                fallback_queue,
                config,
                dispatcher,
                metrics,
                delivery_monitor,
            ),
            name="reasoning",
        ),
        asyncio.create_task(
            _fallback_loop(
                fallback_queue, config, dispatcher, metrics, delivery_monitor
            ),
            name="fallback",
        ),
        asyncio.create_task(_retention_loop(qdrant), name="retention"),
        asyncio.create_task(_heartbeat_loop(redis), name="heartbeat"),
        asyncio.create_task(impact_confirmer.run_forever(), name="impact_confirmer"),
    ]

    try:
        await stop_event.wait()
    finally:
        log.info("besm_shutting_down")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await scheduler.stop()
        await dispatcher.aclose()
        await redis.aclose()
        qdrant.close()
        log.info("besm_stopped")


async def _reasoning_loop(
    scored_queue: asyncio.Queue[ScoredSignal],
    fallback_queue: asyncio.Queue[ScoredSignal],
    config,
    dispatcher: Dispatcher,
    metrics: MetricsCollector,
    delivery_monitor: DeliveryMonitor,
) -> None:
    while True:
        signal = await scored_queue.get()
        if signal.suppressed:
            scored_queue.task_done()
            continue
        asyncio.create_task(
            _process_and_dispatch(
                signal, config, dispatcher, metrics, delivery_monitor, fallback_queue
            )
        )
        scored_queue.task_done()


async def _process_and_dispatch(
    signal: ScoredSignal,
    config,
    dispatcher: Dispatcher,
    metrics: MetricsCollector,
    delivery_monitor: DeliveryMonitor,
    fallback_queue: RedisFallbackQueue,
) -> None:
    from besm.gateway.dispatcher import DispatchResult

    t0 = time.monotonic()
    payload = await process_signal(signal, config.llm, fallback_queue)
    latency = time.monotonic() - t0

    metrics.signal_processed(
        signal_id=signal.signal_id,
        domain=signal.domain,
        confidence=signal.confidence_score,
        lead_time_hours=signal.lead_time_hours,
        suppressed=signal.suppressed,
        latency_s=latency,
    )

    if payload is None:
        # Reasoning layer failed — not a channel delivery failure
        return

    result = await dispatcher.dispatch(payload)

    # Only record to delivery monitor when a send was actually attempted
    if result in (DispatchResult.DELIVERED, DispatchResult.FAILED):
        delivery_monitor.record(
            config.alerts.primary_channel,
            result is DispatchResult.DELIVERED,
        )

    metrics.alert_delivered(
        alert_id=payload["alert_id"],
        channel=config.alerts.primary_channel,
        success=result is DispatchResult.DELIVERED,
    )


async def _fallback_loop(
    fallback_queue: RedisFallbackQueue,
    config,
    dispatcher: Dispatcher,
    metrics: MetricsCollector,
    delivery_monitor: DeliveryMonitor,
) -> None:
    while True:
        await asyncio.sleep(300)
        while True:
            signal = await fallback_queue.pop()
            if signal is None:
                break
            log.info("fallback_retry", signal_id=signal.signal_id)
            asyncio.create_task(
                _process_and_dispatch(
                    signal,
                    config,
                    dispatcher,
                    metrics,
                    delivery_monitor,
                    fallback_queue,
                )
            )


async def _retention_loop(qdrant) -> None:
    while True:
        await asyncio.sleep(60 * 60 * 24)
        await asyncio.to_thread(run_retention_cleanup, qdrant)


async def _heartbeat_loop(redis: Redis) -> None:
    while True:
        await redis.set("besm:heartbeat", "1", ex=120)
        await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
