# BESM — Butterfly Effect Signal Monitor

Single-operator agentic intelligence system that detects weak public signals before they manifest as real-world impacts, using **Temporal Arbitrage**: the Lead Time between a Signal and its predicted Impact.

## Key Principles

- **Signal-first**: raw data is only kept if it implies a future impact.
- **Lead Time is the product**: every alert carries a time window.
- **Confidence before delivery**: alerts require Confidence Score ≥ 40.
- **Local-first**: no cloud dependencies for core logic; vector store, config, and agent state run locally.
- **Operator-controlled**: `config.yaml` is the single control surface.
- **LLM as a data source**: ingestion nodes query an LLM with web search instead of scraping HTML directly.

## Architecture

Event-driven, microservice-oriented, single-operator (no auth, no multi-tenancy):

```
Ingestion Fleet (APScheduler) → LLM Backend (web search)
    → Redis Streams (raw_signals)
    → Butterfly Engine (dedup → embed → similarity search → score → lead time)
    → Reasoning Layer (LangGraph supervisor + domain agents)
    → Action Gateway (Telegram / WhatsApp / Push)
```

Background services: `ImpactConfirmer` (verifies predicted impacts) and `Retention Cleanup` (purges Qdrant records > 3 years old).

## Components

| Layer | Responsibility |
|---|---|
| Ingestion Nodes | Domain-specific `BaseIngestionNode` subclasses; fetch via LLM web search or Anthropic Batch API |
| Butterfly Engine | Dedup, embedding, vector similarity, confidence scoring, lead-time calculation |
| Reasoning Layer | LangGraph supervisor routes to domain agents (commodity, financial, legal, health, urban, market) |
| Vector Store | Qdrant (embedded) — `signals` and `butterfly_chains` collections |
| Action Gateway | Formats and dispatches alerts via Telegram/WhatsApp/Push with retry and rate limiting |

## Domains & Nodes

- **Commodity**: mandi prices, IMD rainfall, import duty, MRP monitor
- **Financial**: forex, Brent crude, GMP, IPO subscription, gold sentiment, panchang
- **Legal**: gazette, state portal, slot sentinel, power outage, flight status
- **Health**: AQI, IMD weather, social sentiment
- **Urban**: geotagged social signals, traffic
- **Market**: job board, municipal gazette, infrastructure, business directory

All nodes are enabled/disabled individually in `config.yaml` under `nodes:`.

## Confidence Score Algorithm

```
Base_Score = 50
+ 15  if top-5 cosine similarity avg > 0.7
- 20  if fewer than 3 historical matches with similarity > 0.7
+ 15  if source_count >= 2 (cross-validation)
+ 15  if domain-specific corroborating signal
- 10  if most similar chain is > 2 years old

Final_Score = clamp(sum, 0, 100)
```

Signals scoring below 40 are suppressed and stored for future improvement, not forwarded.

## Tech Stack

| Component | Technology |
|---|---|
| Language | Python 3.11+ |
| Scheduling | APScheduler 3.x (AsyncIOScheduler) |
| Message Bus | Redis Streams |
| Embeddings | `paraphrase-multilingual-mpnet-base-v2` (768-dim) |
| Vector Store | Qdrant (embedded mode) |
| Reasoning | LangGraph (StateGraph) |
| LLM Backend | Anthropic Claude Haiku 4.5 (default) / OpenAI-compatible |
| Channels | python-telegram-bot, Twilio WhatsApp API, ntfy.sh |
| Config | Pydantic v2, hot-reloaded via `LiveConfig` |
| Secrets | python-dotenv (env vars only, never in config/source) |
| Logging | structlog (JSON), JSONL metrics |
| Testing | pytest, pytest-asyncio, Hypothesis (property-based) |

## Configuration

All operator settings live in `config.yaml`:

```yaml
operator:
  city, state, pin_codes, income_bracket, occupation, family_size

alerts:
  primary_channel, secondary_channel
  min_confidence_threshold   # [40, 100]
  daily_limit                # [1, 10]
  quiet_hours_start, quiet_hours_end, timezone

nodes:
  # true/false per ingestion node — single control surface

llm:
  provider, model, api_key_env, base_url, web_search_enabled

vector_store:
  path, embedding_model

system:
  impact_check_interval_hours, impact_confirmation_window_hours
```

Secrets are referenced by environment variable name only:

```
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
TWILIO_ACCOUNT_SID=
TWILIO_AUTH_TOKEN=
TWILIO_WHATSAPP_FROM=
OPERATOR_WHATSAPP_NUMBER=
NTFY_TOPIC=
ANTHROPIC_API_KEY=
```

## Setup

```bash
git clone https://github.com/sujinnair/besm.git
cd besm
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in secrets
cp config.example.yaml config.yaml   # configure operator + nodes
python main.py
```

Requires a local Redis instance reachable at startup (`run_startup_checks()` verifies this).

## Error Handling Highlights

- Ingestion failures retry on the next poll tick; sustained failure > 60 min raises an `OPERATIONAL_ALERT`.
- Duplicate signals (`content_hash` match) are silently dropped.
- Signals with lead time ≤ 0 or confidence < 40 are suppressed and stored, not dispatched.
- Primary channel delivery failures retry on the secondary channel within 10 minutes.
- Delivery success rate < 0.90 in a rolling 1-hour window raises an `OPERATIONAL_ALERT`.

## License

MIT License

Copyright (c) 2026 Sujin S

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
