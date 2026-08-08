from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator


class LocationConfig(BaseModel):
    label: str = "home"
    city: str
    state: str
    pin_codes: list[str] = Field(default_factory=list)
    localities: list[str] = Field(default_factory=list)
    airport_iata: str | None = None
    primary: bool = True

    @field_validator("pin_codes")
    @classmethod
    def validate_pin_codes(cls, v: list[str]) -> list[str]:
        for pin in v:
            if not pin.isdigit() or len(pin) != 6:
                raise ValueError(f"Invalid PIN code: {pin!r}")
        return v


class OperatorConfig(BaseModel):
    locations: list[LocationConfig] = Field(min_length=1)
    income_bracket: str
    occupation: str
    family_size: int = Field(ge=1)

    @model_validator(mode="after")
    def ensure_one_primary(self) -> "OperatorConfig":
        primaries = [loc for loc in self.locations if loc.primary]
        if len(primaries) == 0:
            self.locations[0].primary = True
        elif len(primaries) > 1:
            raise ValueError("Only one location may be marked as primary: true")
        return self

    @property
    def primary_location(self) -> LocationConfig:
        for loc in self.locations:
            if loc.primary:
                return loc
        return self.locations[0]

    def location_vars(self) -> dict[str, str]:
        primary = self.primary_location
        secondary = [loc for loc in self.locations if not loc.primary]

        localities_str = ", ".join(primary.localities) if primary.localities else primary.city
        all_locations = " and ".join(f"{loc.city}, {loc.state}" for loc in self.locations)
        all_cities = " and ".join(loc.city for loc in self.locations)

        travel_note = ""
        if secondary:
            travel_locs = ", ".join(
                f"{loc.city}, {loc.state} ({loc.label})" for loc in secondary
            )
            travel_note = f" Also currently monitoring {travel_locs}."

        return {
            "primary_city": primary.city,
            "primary_state": primary.state,
            "primary_city_state": f"{primary.city}, {primary.state}",
            "localities": localities_str,
            "airport_iata": primary.airport_iata or f"{primary.city} airport",
            "all_locations": all_locations,
            "all_cities": all_cities,
            "travel_note": travel_note,
        }


class AlertsConfig(BaseModel):
    primary_channel: Literal["telegram", "whatsapp", "push"]
    secondary_channel: Literal["telegram", "whatsapp", "push"] | None = None
    min_confidence_threshold: int = Field(ge=40, le=100)
    daily_limit: int = Field(ge=1, le=10)
    quiet_hours_start: str = "22:00"
    quiet_hours_end: str = "07:00"
    timezone: str = "Asia/Kolkata"

    @model_validator(mode="before")
    @classmethod
    def channels_present(cls, values: dict) -> dict:
        if not values.get("primary_channel"):
            raise ValueError(
                "alerts.primary_channel is required. "
                "Valid values: 'telegram', 'whatsapp', 'push'."
            )
        return values

    @model_validator(mode="after")
    def channels_must_differ(self) -> "AlertsConfig":
        if (
            self.secondary_channel is not None
            and self.primary_channel == self.secondary_channel
        ):
            raise ValueError("primary_channel and secondary_channel must differ")
        return self

    @field_validator("quiet_hours_start", "quiet_hours_end")
    @classmethod
    def validate_time_format(cls, v: str) -> str:
        parts = v.split(":")
        if len(parts) != 2 or not all(p.isdigit() for p in parts):
            raise ValueError(f"Time must be in HH:MM format, got {v!r}")
        h, m = int(parts[0]), int(parts[1])
        if not (0 <= h <= 23 and 0 <= m <= 59):
            raise ValueError(f"Invalid time value: {v!r}")
        return v


class NodesConfig(BaseModel):
    # Commodity
    commodity_mandi: bool = True
    commodity_imd_rainfall: bool = True
    commodity_import_duty: bool = True
    commodity_mrp_monitor: bool = True
    # Financial
    financial_forex: bool = True
    financial_brent_crude: bool = True
    financial_gmp: bool = True
    financial_ipo_subscription: bool = True
    financial_gold_sentiment: bool = True
    financial_panchang: bool = True
    # Legal
    legal_gazette: bool = True
    legal_state_portal: bool = True
    legal_slot_sentinel: bool = True
    legal_power_outage: bool = True
    legal_flight_status: bool = False
    # Health
    health_aqi: bool = True
    health_imd_weather: bool = True
    health_social_sentiment: bool = True
    # Urban
    urban_social_geotagged: bool = True
    urban_traffic: bool = True
    # Market
    market_job_board: bool = True
    market_municipal_gazette: bool = True
    market_infrastructure: bool = True
    market_business_directory: bool = True


class CommodityConfig(BaseModel):
    packaged_goods_categories: list[str] = Field(default_factory=list)


class MonitoredFlight(BaseModel):
    flight_number: str
    date: str


class FinancialConfig(BaseModel):
    monitored_flights: list[MonitoredFlight] = Field(default_factory=list)


class MonitoredPortal(BaseModel):
    portal: Literal["passport_seva", "rto", "vfs"]
    service_type: str


class LegalConfig(BaseModel):
    monitored_portals: list[MonitoredPortal] = Field(default_factory=list)


class HealthConfig(BaseModel):
    flu_forecast_pin_codes: list[str] = Field(default_factory=list, max_length=3)
    aqi_sensitivity_threshold: int = Field(ge=100, le=300)

    @field_validator("flu_forecast_pin_codes")
    @classmethod
    def validate_pin_codes(cls, v: list[str]) -> list[str]:
        for pin in v:
            if not pin.isdigit() or len(pin) != 6:
                raise ValueError(f"Invalid PIN code: {pin!r}")
        return v


class MonitoredLocation(BaseModel):
    name: str
    type: Literal["road_corridor", "venue"]


class UrbanConfig(BaseModel):
    monitored_locations: list[MonitoredLocation] = Field(
        default_factory=list, max_length=10
    )


class MarketConfig(BaseModel):
    zoning_zones: list[str] = Field(default_factory=list, max_length=5)
    business_gap_zones: list[str] = Field(default_factory=list, max_length=3)
    skill_demand_skills: list[str] = Field(default_factory=list, max_length=10)


class TelegramChannelConfig(BaseModel):
    enabled: bool = True
    bot_token_env: str
    chat_id_env: str


class WhatsAppChannelConfig(BaseModel):
    enabled: bool = True
    account_sid_env: str
    auth_token_env: str
    from_number_env: str
    to_number_env: str


class PushChannelConfig(BaseModel):
    enabled: bool = True
    ntfy_topic_env: str
    ntfy_server: str = "https://ntfy.sh"


class ChannelsConfig(BaseModel):
    telegram: TelegramChannelConfig | None = None
    whatsapp: WhatsAppChannelConfig | None = None
    push: PushChannelConfig | None = None


class LLMBackendConfig(BaseModel):
    provider: Literal["anthropic", "openai", "groq", "ollama", "gemini"] = "anthropic"
    model: str = "claude-sonnet-4-20250514"
    api_key_env: str = "ANTHROPIC_API_KEY"
    base_url: str = "https://api.anthropic.com/v1"
    web_search_enabled: bool = True


class VectorStoreConfig(BaseModel):
    path: str = "./data/qdrant"
    embedding_model: str = "paraphrase-multilingual-mpnet-base-v2"


class LoggingConfig(BaseModel):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    log_file: str = "./logs/besm.log"
    metrics_file: str = "./logs/metrics.jsonl"


class SystemConfig(BaseModel):
    impact_check_interval_hours: int = Field(default=6, ge=1, le=24)
    impact_confirmation_window_hours: int = Field(default=24, ge=1, le=72)


class BESMConfig(BaseModel):
    operator: OperatorConfig
    alerts: AlertsConfig
    nodes: NodesConfig = Field(default_factory=NodesConfig)
    commodity: CommodityConfig = Field(default_factory=CommodityConfig)
    financial: FinancialConfig = Field(default_factory=FinancialConfig)
    legal: LegalConfig = Field(default_factory=LegalConfig)
    health: HealthConfig
    urban: UrbanConfig = Field(default_factory=UrbanConfig)
    market: MarketConfig = Field(default_factory=MarketConfig)
    channels: ChannelsConfig = Field(default_factory=ChannelsConfig)
    vector_store: VectorStoreConfig = Field(default_factory=VectorStoreConfig)
    llm: LLMBackendConfig = Field(default_factory=LLMBackendConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    system: SystemConfig = Field(default_factory=SystemConfig)

    @model_validator(mode="after")
    def required_channel_configs_present(self) -> "BESMConfig":
        channels = [self.alerts.primary_channel]
        if self.alerts.secondary_channel is not None:
            channels.append(self.alerts.secondary_channel)
        for channel in channels:
            if channel == "telegram":
                if self.channels.telegram is None:
                    raise ValueError("channels.telegram must be configured")
                if not self.channels.telegram.enabled:
                    raise ValueError(
                        "channels.telegram is disabled but set as primary or secondary"
                    )
            if channel == "whatsapp":
                if self.channels.whatsapp is None:
                    raise ValueError("channels.whatsapp must be configured")
                if not self.channels.whatsapp.enabled:
                    raise ValueError(
                        "channels.whatsapp is disabled but set as primary or secondary"
                    )
            if channel == "push":
                if self.channels.push is None:
                    raise ValueError("channels.push must be configured")
                if not self.channels.push.enabled:
                    raise ValueError(
                        "channels.push is disabled but set as primary or secondary"
                    )
        return self
