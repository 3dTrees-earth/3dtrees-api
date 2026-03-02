"""
Compatibility shim.

The API app is assembled in `trees_api.app.server`.
This module keeps legacy imports/entrypoints stable.
"""

from trees_api.app.server import (  # noqa: F401
    APIServerSettings,
    APP_START_TIME,
    app,
    connection_manager,
    get_galaxy_client,
    get_storage_client,
    get_supabase_client,
    health_check,
    info,
    main,
    version_info,
)


if __name__ == "__main__":
    main()

