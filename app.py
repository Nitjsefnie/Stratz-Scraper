import os

from stratz_scraper import create_app
from stratz_scraper.database import ensure_database_exists

ensure_database_exists()
app = create_app()


if __name__ == "__main__":
    host = os.environ.get("STRATZ_BIND_HOST", "0.0.0.0")
    port = int(os.environ.get("STRATZ_BIND_PORT", "80"))
    app.run(host=host, port=port, debug=False, threaded=True)
