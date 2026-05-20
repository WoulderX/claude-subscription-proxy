from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from mitmproxy import options
from mitmproxy.tools.dump import DumpMaster

from .addon import HijackAddon

if TYPE_CHECKING:
    from ..session.session import ClaudeSession

log = logging.getLogger(__name__)


class MitmRunner:
    """Embedded mitmproxy DumpMaster, one per ClaudeSession, on its own
    loopback port. Runs as an asyncio task in the main event loop so it
    shares memory with the API server and SessionManager."""

    def __init__(self, port: int, session: "ClaudeSession") -> None:
        self.port = port
        self.session = session
        self.master: DumpMaster | None = None
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        opts = options.Options(
            listen_host="127.0.0.1",
            listen_port=self.port,
            # Plain HTTP proxy mode — claude code uses HTTPS_PROXY=http://...
            mode=["regular"],
        )
        master = DumpMaster(opts, with_termlog=False, with_dumper=False)
        master.addons.add(HijackAddon(self.session))
        self.master = master
        self._task = asyncio.create_task(master.run(), name=f"mitm-{self.session.user_id}")
        # Wait briefly for the listener to bind.
        await asyncio.sleep(0.3)
        log.info("mitm listener up user=%s port=%s", self.session.user_id, self.port)

    async def stop(self) -> None:
        if self.master is not None:
            self.master.shutdown()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=5)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
