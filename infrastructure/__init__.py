# lang: Python 3.11+, file: __init__.py, target: Windows 11 / Linux
# Infrastructure package — proxy management, rate limiting, flood protection.

from infrastructure.proxy_manager import (
    ProxyProtocol,
    ProxyStatus,
    RotationPolicy,
    BanSignal,
    ProxyCredential,
    ProxyEntry,
    HealthCheckResult,
    HealthChecker,
    ProxyStore,
    ProxyManager,
    MultiPoolManager,
    ProxyScanner,
    BanDetector,
    PoolStats,
    proxy_to_telethon_dict,
    build_telethon_proxies,
    build_proxy_manager,
    assign_proxies_to_accounts,
    export_working_proxies,
)

from infrastructure.rate_controller import (
    ActionType,
    Priority,
    LimitResult,
    RateWindow,
    TokenBucket,
    ActionLimitSpec,
    AccountLimiter,
    RateController,
    AdaptiveBackoff,
    MultiAccountRateCoordinator,
    ThrottledCaller,
    aggressive_specs,
    conservative_specs,
    build_rate_controller,
)

from infrastructure.flood_guard import (
    FloodLevel,
    CircuitState,
    GuardEvent,
    FloodRecord,
    CircuitStats,
    FloodSummary,
    CircuitBreaker,
    AccountFloodTracker,
    RecoveryScheduler,
    FloodGuard,
    FloodBlockedError,
    FloodAwareThrottler,
    build_flood_guard,
    flood_level_description,
    circuit_state_description,
    print_flood_report,
)

__all__ = [
    # proxy_manager
    "ProxyProtocol", "ProxyStatus", "RotationPolicy", "BanSignal",
    "ProxyCredential", "ProxyEntry", "HealthCheckResult", "HealthChecker",
    "ProxyStore", "ProxyManager", "MultiPoolManager", "ProxyScanner",
    "BanDetector", "PoolStats",
    "proxy_to_telethon_dict", "build_telethon_proxies", "build_proxy_manager",
    "assign_proxies_to_accounts", "export_working_proxies",
    # rate_controller
    "ActionType", "Priority", "LimitResult", "RateWindow", "TokenBucket",
    "ActionLimitSpec", "AccountLimiter", "RateController", "AdaptiveBackoff",
    "MultiAccountRateCoordinator", "ThrottledCaller",
    "aggressive_specs", "conservative_specs", "build_rate_controller",
    # flood_guard
    "FloodLevel", "CircuitState", "GuardEvent", "FloodRecord",
    "CircuitStats", "FloodSummary", "CircuitBreaker", "AccountFloodTracker",
    "RecoveryScheduler", "FloodGuard", "FloodBlockedError", "FloodAwareThrottler",
    "build_flood_guard", "flood_level_description", "circuit_state_description",
    "print_flood_report",
]