#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────────────────
# install_clientbot.sh — set up discord.py-self alongside discord.py
#
# CHATAFT supports two Discord modes: bot (default) and clientbot (selfbot).
# Clientbot mode uses a real user account via the discord.py-self fork. Both
# libraries install as the 'discord' package, so they normally collide. This
# script installs discord.py-self into the active Python venv's site-packages
# under a RENAMED top-level folder ('selfcord') and rewrites all internal
# absolute imports to match, so both libraries can coexist in one venv.
#
# Run this once per venv. Safe to re-run (it's idempotent — reinstalls clean).
#
# WARNING: Running CHATAFT in clientbot mode automates a user account, which
# violates Discord's Terms of Service and can result in account termination.
# The maintainer offers this as an opt-in feature; users assume all risk.
# ────────────────────────────────────────────────────────────────────────────

set -euo pipefail

# ─── sanity checks ──────────────────────────────────────────────────────────

if [[ -z "${VIRTUAL_ENV:-}" ]]; then
    echo "ERROR: no Python virtualenv is active."
    echo "Activate your bot's venv first (e.g. 'source ~/botenv/bin/activate')"
    echo "then re-run this script."
    exit 1
fi

echo "Using venv: $VIRTUAL_ENV"

SITE=$(python -c "import site; print(site.getsitepackages()[0])")
echo "Target site-packages: $SITE"

if [[ ! -d "$SITE" ]]; then
    echo "ERROR: site-packages path does not exist: $SITE"
    exit 1
fi

# ─── cleanup any prior install ──────────────────────────────────────────────

echo ""
echo "[1/6] Removing any previous clientbot library install..."

# Remove Omkaar's unrelated selfcord.py if someone installed it by mistake
pip uninstall -y selfcord.py 2>/dev/null || true

# Remove our renamed folder if a prior run of this script left one
if [[ -d "$SITE/selfcord" ]]; then
    echo "       found existing $SITE/selfcord - removing"
    rm -rf "$SITE/selfcord"
fi

# Remove the metadata dir from a prior discord.py-self install into main site-packages
# (if someone pip installed it directly, it would collide with discord.py).
# Only target dist-info/egg-info dirs for the self-fork, not discord.py's own.
find "$SITE" -maxdepth 1 -type d \( -name 'discord.py_self-*.dist-info' -o -name 'discord_py_self-*.dist-info' \) -exec rm -rf {} + 2>/dev/null || true

# Nuke any leftover temp folders from interrupted runs
rm -rf "$SITE/_selfcord_tmp" 2>/dev/null || true

# ─── install discord.py-self into a temp folder ─────────────────────────────

echo ""
echo "[2/6] Installing discord.py-self into a temp folder..."
pip install --quiet discord.py-self --no-deps --target "$SITE/_selfcord_tmp"

if [[ ! -d "$SITE/_selfcord_tmp/discord" ]]; then
    echo "ERROR: pip install did not produce expected discord/ folder"
    exit 1
fi

# ─── rename the package folder ──────────────────────────────────────────────

echo ""
echo "[3/6] Renaming discord/ → selfcord/..."
mv "$SITE/_selfcord_tmp/discord" "$SITE/selfcord"
rm -rf "$SITE/_selfcord_tmp"

# ─── install discord.py-self's runtime deps into the venv normally ──────────
# These don't collide with anything in discord.py, so a regular pip install is fine.

echo ""
echo "[4/6] Installing runtime deps (discord-protos, curl_cffi)..."
pip install --quiet discord-protos || {
    echo "WARN: discord-protos install failed - may already be present, or may need manual install"
}
pip install --quiet curl_cffi || {
    echo "WARN: curl_cffi install failed - may already be present, or may need manual install"
}

# ─── rewrite absolute 'discord' references to 'selfcord' ────────────────────
# discord.py-self's source has absolute imports and attribute references like
# 'import discord.abc' and 'class User(discord.abc.Connectable)' which all
# point at the wrong library (or nothing) after the folder rename. Three
# sed passes fix this:
#   Pass 1 — rewrite 'import discord...' and 'from discord...' statements
#   Pass 2 — rewrite all remaining 'discord.X' attribute accesses
#   Pass 3 — restore brand/URL strings that must stay literal (discord.com etc.)

echo ""
echo "[5/6] Rewriting internal 'discord' references to 'selfcord'..."

# Pass 1: import statements
find "$SITE/selfcord" -name "*.py" -exec sed -i -E \
    -e 's/^([[:space:]]*)import discord\b/\1import selfcord/g' \
    -e 's/^([[:space:]]*)from discord\b/\1from selfcord/g' \
    {} +

# Pass 2: all remaining discord.X attribute references (class bases, type hints, etc.)
find "$SITE/selfcord" -name "*.py" -exec sed -i \
    's/\bdiscord\./selfcord./g' {} +

# Pass 3: restore literal brand/URL references that got over-replaced
# These appear inside string literals, regex patterns, and docstrings — they
# refer to Discord's actual domains and must stay as "discord".
find "$SITE/selfcord" -name "*.py" -exec sed -i -E \
    's/\bselfcord\.(com|gg|gift|new|py)\b/discord.\1/g' {} +

# ─── bust the bytecode cache ────────────────────────────────────────────────
# Compiled .pyc files still contain the old references — delete them so
# Python recompiles from the fixed source on next import.

find "$SITE/selfcord" -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

# ─── verify ─────────────────────────────────────────────────────────────────

echo ""
echo "[6/6] Verifying install..."

python - <<'PYEOF'
import sys
try:
    import selfcord
except Exception as e:
    print(f"FAIL: import selfcord raised {type(e).__name__}: {e}")
    sys.exit(1)

try:
    c = selfcord.Client()
except Exception as e:
    print(f"FAIL: selfcord.Client() raised {type(e).__name__}: {e}")
    sys.exit(1)

print(f"  selfcord.__file__: {selfcord.__file__}")
print(f"  Client class:      {type(c).__module__}.{type(c).__name__}")
print(f"  Has Webhook:       {'Webhook' in dir(selfcord)}")
print(f"  Has DMChannel:     {'DMChannel' in dir(selfcord)}")
print(f"  Has MessageType:   {'MessageType' in dir(selfcord)}")
print("")
print("  ALL GOOD - selfcord is ready for CHATAFT clientbot mode.")
PYEOF

echo ""
echo "─────────────────────────────────────────────────────────────────────"
echo " Install complete. To activate clientbot mode:"
echo ""
echo "   1. In codex.ini, under [Features] add:"
echo "        DISCORD_MODE = clientbot"
echo ""
echo "   2. In codex.ini, under [Credentials] set DISCORD_TOKEN to your"
echo "      Discord USER token (not a bot token)."
echo ""
echo "   3. Ensure each bridge gateway has a pre-created webhook_url in"
echo "      its gateway config - selfbots cannot create webhooks via API."
echo ""
echo "   4. Start the bot as usual. Look for the red ToS warning banner"
echo "      in the startup logs to confirm clientbot mode is active."
echo "─────────────────────────────────────────────────────────────────────"
