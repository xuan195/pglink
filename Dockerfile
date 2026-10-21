# syntax=docker/dockerfile:1
FROM node:26-alpine@sha256:2d984a15c9b54fd0aeb608b8e0d0d83529eb34d2966db27a1fb4f1edc3d298a3 AS frontend-builder

WORKDIR /app
COPY explorer/package*.json ./explorer/
WORKDIR /app/explorer
RUN npm ci

COPY explorer/ ./
RUN mkdir -p /app/semantica && npm run build

FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    FALKORDB_HOST=falkordb \
    FALKORDB_PORT=6379 \
    ALLOWED_ORIGINS=http://localhost:8000,http://127.0.0.1:8000

WORKDIR /app

# Debian trixie-security already ships fixed builds for these base-image OS
# packages (Trivy library/semantica alerts, all CVE-2026-*):
# - #6151-#6162: perl-base (7 CVEs across perl core, Storable, Archive::Tar
#   and IO::Compress - all fixed by the same upstream perl source upload),
#   libpcre2-8-0 (2 CVEs), libsqlite3-0 (2 CVEs, FTS5), and gzip (1 CVE, LZH
#   decompression).
# - #6167-#6172: openssl, libssl3t64 and openssl-provider-legacy (one source
#   package, fixed in 3.5.7-1~deb13u3) for CVE-2026-84782 (DTLS handshake
#   retransmission out-of-bounds read) and CVE-2026-75804 (QUIC connection
#   flow control not enforced). Neither is reachable here - uvicorn serves
#   plain HTTP/TCP, no DTLS or QUIC - but patching clears the alerts.
#
# --only-upgrade scopes this to just the named packages instead of a
# blanket `apt-get upgrade` (terrascan AC_DOCKER_0052 - that breaks build
# reproducibility), but deliberately WITHOUT a `pkg=version` pin like the
# setuptools pin below: unlike PyPI, Debian's live mirrors only ever serve the current
# point release of a package, not every historical one. A pin to today's
# fixed version (e.g. perl-base=5.40.1-6+deb13u1) would 404 the day Debian
# ships deb13u2 and break every build that hits this layer - CI, Cloud
# Build, and local Compose alike. Leaving the version unpinned means apt
# always resolves to whatever trixie-security currently has, which is
# guaranteed >= today's fixed version since security repos never regress.
RUN apt-get update \
    && apt-get install -y --no-install-recommends --only-upgrade \
        perl-base \
        libpcre2-8-0 \
        libsqlite3-0 \
        gzip \
        openssl \
        libssl3t64 \
        openssl-provider-legacy \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --system semantica \
    && useradd --system --gid semantica --home-dir /app --shell /usr/sbin/nologin semantica

COPY pyproject.toml README.md LICENSE MANIFEST.in \
     .github/requirements/explorer-extra-py313.txt .github/requirements/pep517-build.txt ./
COPY semantica/ ./semantica/
COPY integrations/ ./integrations/
COPY --from=frontend-builder /app/semantica/static ./semantica/static

RUN pip install --no-cache-dir --timeout 120 --retries 10 \
    -i https://pypi.tuna.tsinghua.edu.cn/simple \
    ".[explorer,llm-deepseek,graph-neo4j]" \
    && chown -R semantica:semantica /app

USER semantica

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import json, urllib.request; data=json.load(urllib.request.urlopen('http://127.0.0.1:8000/api/health', timeout=3)); raise SystemExit(0 if data.get('status') == 'ok' else 1)"

CMD ["python", "-m", "uvicorn", "semantica.explorer.app:app", "--host", "0.0.0.0", "--port", "8000"]
