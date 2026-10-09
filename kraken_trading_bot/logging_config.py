# logging_config.py
"""
Centralized Logging Configuration Module

Provides unified logging setup for all trading bot modules with:
- Rotating file handler (5MB max, 5 backups)
- Optional console output
- Per-module log level configuration
- Config file integration (reads from config.json)

This module ensures all logs go to a single file with consistent formatting,
while allowing different verbosity levels for different modules.

Example usage:
    ```python
    from logging_config import setup_centralized_logging, get_logger

    # Initialize logging (call once at startup)
    setup_centralized_logging()

    # Get a logger for your module
    logger = get_logger('MyModule')
    logger.info("Module initialized")
    logger.debug("Detailed debug info")
    ```

Config file example (config.json):
    ```json
    {
        "logging": {
            "level": "INFO",
            "console_logging": true,
            "module_levels": {
                "KeyLoader": "WARNING",
                "DataManager": "INFO",
                "ExchangeConnection": "INFO",
                "LiveTradingLoop": "INFO"
            }
        }
    }
    ```

Note:
    - Call `setup_centralized_logging()` once at application startup
    - Use `get_logger(name)` in each module to get a configured logger
    - Log files are written to `logs/trading_bot.log` by default
    - The 'logging' section (and required keys) must be present in config.json.
      Missing configuration will raise an exception (loud failure, no silent defaults).
"""

import logging
import logging.handlers
import os
from typing import Dict


# Level name to logging constant mapping
LEVEL_MAP: Dict[str, int] = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}


def setup_centralized_logging(config: dict, log_file: str = "logs/trading_bot.log") -> None:
    """Configure centralized logging from the bot's config dict.

    Reads `config['logging']` for level, module_levels, and (optional)
    console_logging. Fails loud on missing/invalid keys — no defaults.
    """

    if 'logging' not in config:
        raise RuntimeError(
            "Missing required 'logging' section in config. "
            "No silent fallback."
        )
    config_settings = config['logging']

    # 'level' is mandatory
    if 'level' not in config_settings:
        raise RuntimeError(
            "Missing required 'level' key inside the 'logging' section of config.json. "
            "No silent default to INFO."
        )

    level_str = config_settings['level'].upper()
    log_level = LEVEL_MAP.get(level_str)
    if log_level is None:
        raise RuntimeError(
            f"Invalid logging level '{config_settings['level']}' in config.json. "
            f"Must be one of {list(LEVEL_MAP.keys())}."
        )

    if 'console_logging' not in config_settings:
        raise RuntimeError(
            "Missing required 'console_logging' key inside the 'logging' "
            "section of config.json."
        )
    console_logging = bool(config_settings['console_logging'])

    # module_levels is now mandatory
    if 'module_levels' not in config_settings:
        raise RuntimeError(
            "Missing required 'module_levels' key inside the 'logging' section of config.json. "
            "No silent hardcoded defaults."
        )

    module_levels = {}
    disabled_modules = set()
    for module_name, level_str in config_settings['module_levels'].items():
        upper_level = level_str.upper()
        if upper_level == 'OFF':
            disabled_modules.add(module_name)
            continue
        if upper_level not in LEVEL_MAP:
            raise RuntimeError(
                f"Invalid log level '{level_str}' for module '{module_name}' in config.json. "
                f"Must be one of {list(LEVEL_MAP.keys())} or 'OFF'."
            )
        module_levels[module_name] = LEVEL_MAP[upper_level]

    # Explicitly create all configured logger objects here.
    # This centralizes logger creation instead of having modules create
    # their own loggers on first use.
    for module_name in list(module_levels.keys()) + list(disabled_modules):
        logging.getLogger(module_name)

    # Ensure logs directory exists
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    # Create root logger configuration
    root_logger = logging.getLogger()

    # Clear any existing handlers to avoid duplicates
    root_logger.handlers.clear()

    # Root logger level controls the floor for all loggers that don't have
    # their own level. Per-module overrides in `module_levels` apply only
    # to those specific modules. (Setting root to min(module_levels) was
    # backwards — it raised the floor when a single quieted module was
    # configured, silently dropping every other module's INFO logs.)
    root_logger.setLevel(log_level)

    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Create file handler with rotation
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=5*1024*1024,  # 5MB
        backupCount=5
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)  # File gets all levels
    root_logger.addHandler(file_handler)

    # Create console handler if requested
    if console_logging:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(log_level)
        root_logger.addHandler(console_handler)

    # Configure levels and propagation for all centrally-created loggers
    for module_name, level in module_levels.items():
        module_logger = logging.getLogger(module_name)
        module_logger.setLevel(level)

    for module_name in module_levels.keys():
        logging.getLogger(module_name).propagate = True

    # Silence modules whose config level is "OFF".
    for module_name in disabled_modules:
        logging.getLogger(module_name).disabled = True

def get_logger(name: str) -> logging.Logger:
    """
    Get a logger for the specified module.

    Returns a logger instance configured by `setup_centralized_logging()`.
    If logging hasn't been set up, returns an unconfigured logger that
    will use Python's default behavior.

    Args:
        name: Logger name, typically the module name (e.g., 'KeyLoader',
            'ExchangeConnection'). Used to identify log source and apply
            module-specific log levels.

    Returns:
        A `logging.Logger` instance for the specified name.

    Example:
        ```python
        from logging_config import get_logger

        logger = get_logger('MyModule')
        logger.info("Operation completed")
        logger.debug("Detailed info: %s", data)
        logger.error("Something failed: %s", error)
        ```

    Note:
        - Same name always returns the same logger instance (Python behavior)
        - Logger inherits settings from `setup_centralized_logging()`
        - Safe to call before `setup_centralized_logging()` (logs go to stderr)
    """
    return logging.getLogger(name)
