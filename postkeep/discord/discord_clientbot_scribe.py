"""
Discord clientbot (selfbot) scribe.

Subclasses DiscordScribe and reuses ~all of its event-handling logic. The parent
already reads the discord library reference from `core.discord_lib` (which is
`selfcord` when DISCORD_MODE=clientbot), so message/channel/type checks route
through selfcord automatically.

Place overrides here when a real selfcord quirk surfaces. Default behavior is
full inheritance, so main-scribe changes propagate retroactively.

WARNING: Running this against a user token violates Discord's Terms of Service
and can result in account termination. Use at your own risk.
"""

from postkeep.discord.discord_scribe import DiscordScribe


class DiscordClientbotScribe(DiscordScribe):
    def __init__(self, core):
        super().__init__(core)
        self.component_name = 'discord_clientbot_scribe'
