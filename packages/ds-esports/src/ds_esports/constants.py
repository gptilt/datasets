"""Constants for the Leaguepedia Cargo API."""

# Leaguepedia is hosted on Fandom and exposes the MediaWiki API plus the Cargo
# extension for structured queries against the wiki's relational data.
LEAGUEPEDIA_API_URL = "https://lol.fandom.com/api.php"

# Politeness contract from the wiki's robot policies. We identify ourselves so
# the maintainers can contact us if our scraper misbehaves, and we cap requests
# at 2/sec.
USER_AGENT = "gptilt-datasets/0.1 (https://github.com/franciscoabsampaio/gptilt; ds-esports)"
MIN_REQUEST_INTERVAL_SECONDS = 0.5

# Cargo's hard cap is 500 rows per query; ask for the max so we minimise the
# request count on full-snapshot scrapes.
CARGO_PAGE_LIMIT = 500
