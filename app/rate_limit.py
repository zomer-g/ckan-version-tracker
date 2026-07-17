from slowapi import Limiter

from app.client_ip import client_ip_key

# Key the per-endpoint rate limits by the REAL client IP (see app/client_ip.py),
# NOT slowapi's default get_remote_address. Behind Cloudflare→Render the direct
# peer is a shared edge address, so get_remote_address would collapse every
# @limiter.limit into a single global bucket; client_ip_key resolves the actual
# per-client IP (Cloudflare-validated) instead.
limiter = Limiter(key_func=client_ip_key)
