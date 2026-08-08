from __future__ import annotations

from besm.reasoning.agents.base import DomainAgent


class CommodityAgent(DomainAgent):
    domain = "commodity"

    @property
    def validation_prompt(self) -> str:
        return (
            "Validate this signal for agricultural price impact. Consider Mandi "
            "arrival volumes, import duty changes, rainfall anomalies, and MRP hikes. "
            "Assess whether retail prices are likely to rise and within what timeframe."
        )


class FinancialAgent(DomainAgent):
    domain = "financial"

    @property
    def validation_prompt(self) -> str:
        return (
            "Validate this signal for financial impact. Consider INR-USD movements, "
            "Brent Crude price changes, IPO/GMP data, and gold market sentiment. "
            "Assess the actionable financial opportunity or risk and the likely window."
        )


class LegalAgent(DomainAgent):
    domain = "legal"

    @property
    def validation_prompt(self) -> str:
        return (
            "Validate this signal for legal or bureaucratic impact. Consider gazette "
            "notifications, government subsidies, citizen compensation rights, slot "
            "availability on government portals, and SLA violations. Assess eligibility "
            "and the action window before the opportunity expires."
        )


class HealthAgent(DomainAgent):
    domain = "health"

    @property
    def validation_prompt(self) -> str:
        return (
            "Validate this signal for health risk. Consider flu or viral outbreak "
            "trends, AQI levels, pollution spikes, and weather-driven health risks. "
            "Assess the severity, affected PIN codes, and recommended protective actions."
        )


class UrbanAgent(DomainAgent):
    domain = "urban"

    @property
    def validation_prompt(self) -> str:
        return (
            "Validate this signal for urban logistics impact. Consider traffic "
            "congestion, crowd density at venues, and road corridor disruptions. "
            "Assess severity, estimated duration, and affected locations."
        )


class MarketAgent(DomainAgent):
    domain = "market"

    @property
    def validation_prompt(self) -> str:
        return (
            "Validate this signal for market or professional opportunity. Consider "
            "zoning changes, skill demand trends, infrastructure announcements, and "
            "business gaps near development zones. Assess the opportunity window and "
            "recommended action."
        )
