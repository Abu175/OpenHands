import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from openhands.agent_server.models import EventPage, EventSortOrder
from openhands.app_server.app_conversation.app_conversation_info_service import (
    AppConversationInfoService,
)
from openhands.app_server.app_conversation.app_conversation_models import (
    AppConversationInfo,
)
from openhands.app_server.conversation_paths import V1_CONVERSATIONS_DIR
from openhands.app_server.event.event_service import EventService
from openhands.app_server.event_callback.event_callback_models import EventKind
from openhands.sdk import Event
from openhands.sdk.utils.paging import page_iterator

# When using storage-metadata hints for the TIMESTAMP_DESC initial-load
# optimisation, load this multiple of `limit` events to absorb any small
# discrepancy between the storage-layer hint timestamp and the logical
# event.timestamp.  A factor of 2 means we load at most 2×limit events
# (e.g. 100 for the default limit of 50) instead of every event in the
# conversation.
_HINT_OVERSHOOT_FACTOR = 2


@dataclass
class EventServiceBase(EventService, ABC):
    """Event Service for getting events - the only check on permissions for events is
    in the strict prefix for storage.
    """

    prefix: Path
    user_id: str | None
    app_conversation_info_service: AppConversationInfoService | None
    app_conversation_info_load_tasks: dict[
        UUID, asyncio.Task[AppConversationInfo | None]
    ]

    @abstractmethod
    def _load_event(self, path: Path) -> Event | None:
        """Get the event at the path given."""

    @abstractmethod
    def _store_event(self, path: Path, event: Event):
        """Store the event given at the path given."""

    @abstractmethod
    def _search_paths(self, prefix: Path) -> list[tuple[Path, datetime | None]]:
        """Return all event paths under *prefix* together with a storage-layer
        timestamp hint for each path (e.g. filesystem mtime, S3 LastModified,
        GCS time_created).  The hint is used to pre-sort and pre-filter the
        candidate set before loading event JSON, keeping the common case to
        O(limit) reads rather than O(N).

        Implementations that cannot cheaply obtain a per-path timestamp should
        return ``None`` as the hint; the base-class will then fall back to
        loading every event (original behaviour).
        """

    async def get_conversation_path(self, conversation_id: UUID) -> Path:
        """Get a path for a conversation. Ensure user_id is included if possible."""
        path = self.prefix
        if self.user_id:
            path /= self.user_id
        elif self.app_conversation_info_service:
            task = self.app_conversation_info_load_tasks.get(conversation_id)
            if task is None:
                task = asyncio.create_task(
                    self.app_conversation_info_service.get_app_conversation_info(
                        conversation_id
                    )
                )
                self.app_conversation_info_load_tasks[conversation_id] = task
            conversation_info = await task
            if conversation_info and conversation_info.created_by_user_id:
                path /= conversation_info.created_by_user_id
        path = path / V1_CONVERSATIONS_DIR / conversation_id.hex
        return path

    async def get_event(self, conversation_id: UUID, event_id: UUID) -> Event | None:
        """Get the event with the given id, or None if not found."""
        conversation_path = await self.get_conversation_path(conversation_id)
        path = conversation_path / f'{event_id.hex}.json'
        loop = asyncio.get_running_loop()
        event: Event = await loop.run_in_executor(None, self._load_event, path)  # type: ignore[arg-type]
        return event

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _load_paths(
        self, loop: asyncio.AbstractEventLoop, paths: list[Path]
    ) -> list[Event | None]:
        return await asyncio.gather(  # type: ignore[return-value]
            *[loop.run_in_executor(None, self._load_event, p) for p in paths]
        )

    async def search_events(
        self,
        conversation_id: UUID,
        kind__eq: EventKind | None = None,
        timestamp__gte: datetime | None = None,
        timestamp__lt: datetime | None = None,
        sort_order: EventSortOrder = EventSortOrder.TIMESTAMP,
        page_id: str | None = None,
        limit: int = 100,
    ) -> EventPage:
        """Search events matching the given filters.

        When the storage backend provides per-path timestamp hints (filesystem
        mtime, S3 LastModified, GCS time_created) **and** no offset page_id is
        active, the method uses those hints to load only the events that are
        likely to appear in the result page — typically O(limit) reads instead
        of O(N).  It always validates against the real ``event.timestamp`` field
        before returning, so a slightly inaccurate hint never produces wrong
        results; in the rare case where hints are too imprecise, the method
        falls back to loading the full event set.
        """
        loop = asyncio.get_running_loop()
        prefix = await self.get_conversation_path(conversation_id)
        path_hints: list[tuple[Path, datetime | None]] = await loop.run_in_executor(
            None, self._search_paths, prefix
        )

        # Convert datetime filters to ISO strings so they can be compared
        # against event.timestamp (which is stored as an ISO 8601 string).
        timestamp_gte_str = timestamp__gte.isoformat() if timestamp__gte else None
        timestamp_lt_str = timestamp__lt.isoformat() if timestamp__lt else None

        # ------------------------------------------------------------------
        # Hint-based early-stopping optimisation
        #
        # Eligible when:
        #   • sort_order is TIMESTAMP_DESC (the only order the REST clients
        #     use for pagination)
        #   • every path has a hint (all-or-nothing: partial hints would skew
        #     the pre-sort)
        #   • no offset page_id (offset pagination assumes a stable full-sort,
        #     which we don't do in the fast path)
        # ------------------------------------------------------------------
        hints_available = bool(path_hints) and all(h is not None for _, h in path_hints)
        use_hint_path = (
            hints_available
            and sort_order == EventSortOrder.TIMESTAMP_DESC
            and not page_id
        )

        if use_hint_path:
            items = await self._search_events_with_hints(
                loop=loop,
                path_hints=path_hints,  # type: ignore[arg-type]
                kind__eq=kind__eq,
                timestamp_gte_str=timestamp_gte_str,
                timestamp_lt_str=timestamp_lt_str,
                limit=limit,
            )
            if items is not None:
                # Hints produced a reliable result — apply final sort and return.
                # next_page_id is always None here: REST clients that use the
                # TIMESTAMP_DESC order paginate via timestamp__lt, not page_id.
                items.sort(key=lambda e: e.timestamp, reverse=True)
                return EventPage(items=items[:limit], next_page_id=None)
        # ------------------------------------------------------------------
        # Full-scan fallback (original behaviour)
        # ------------------------------------------------------------------
        all_paths = [p for p, _ in path_hints]
        raw_events = await self._load_paths(loop, all_paths)

        items_full = []
        for event in raw_events:
            if not event:
                continue
            if kind__eq and event.kind != kind__eq:
                continue
            if timestamp_gte_str and event.timestamp < timestamp_gte_str:
                continue
            if timestamp_lt_str and event.timestamp >= timestamp_lt_str:
                continue
            items_full.append(event)

        if sort_order:
            items_full.sort(
                key=lambda e: e.timestamp,
                reverse=(sort_order == EventSortOrder.TIMESTAMP_DESC),
            )

        # Apply offset-based pagination to items (not paths).
        start_offset = 0
        next_page_id = None
        if page_id:
            start_offset = int(page_id)
            items_full = items_full[start_offset:]
        if len(items_full) > limit:
            next_page_id = str(start_offset + limit)
            items_full = items_full[:limit]

        return EventPage(items=items_full, next_page_id=next_page_id)

    async def _search_events_with_hints(
        self,
        loop: asyncio.AbstractEventLoop,
        path_hints: list[tuple[Path, datetime]],
        kind__eq: EventKind | None,
        timestamp_gte_str: str | None,
        timestamp_lt_str: str | None,
        limit: int,
    ) -> list[Event] | None:
        """Fast path: load only as many events as necessary using hint timestamps.

        Returns a list of matching events (may be slightly over-sized; the
        caller trims to *limit* after a final sort), or ``None`` if the hints
        turned out to be insufficient and the caller should fall back to a full
        scan.

        Strategy
        --------
        1. Pre-filter paths using hints so we skip obviously irrelevant events.
        2. Sort surviving paths by hint DESC (newest first) — the order we need
           to return.
        3. Load the first ``limit × _HINT_OVERSHOOT_FACTOR`` paths.
        4. Validate against actual event.timestamp and apply all filters.
        5. If the result set is large enough, return it.  Otherwise signal the
           caller to fall back (None).
        """
        # Pre-filter by hint when a timestamp bound is active.
        if timestamp_lt_str:
            # Keep paths whose hint is before the upper bound.  Accept hints
            # up to the bound (≤ rather than <) because the hint may lag the
            # logical timestamp by a few milliseconds (write latency).
            lt_hint = datetime.fromisoformat(timestamp_lt_str)
            if lt_hint.tzinfo is None:
                lt_hint = lt_hint.replace(tzinfo=timezone.utc)
            path_hints = [(p, h) for p, h in path_hints if h <= lt_hint]

        if timestamp_gte_str:
            gte_hint = datetime.fromisoformat(timestamp_gte_str)
            if gte_hint.tzinfo is None:
                gte_hint = gte_hint.replace(tzinfo=timezone.utc)
            path_hints = [(p, h) for p, h in path_hints if h >= gte_hint]

        # Sort by hint DESC so we load the most-recently-created events first.
        path_hints.sort(key=lambda x: x[1], reverse=True)

        candidate_paths = [p for p, _ in path_hints[: limit * _HINT_OVERSHOOT_FACTOR]]
        raw_events = await self._load_paths(loop, candidate_paths)

        matching: list[Event] = []
        for event in raw_events:
            if not event:
                continue
            if kind__eq and event.kind != kind__eq:
                continue
            if timestamp_gte_str and event.timestamp < timestamp_gte_str:
                continue
            if timestamp_lt_str and event.timestamp >= timestamp_lt_str:
                continue
            matching.append(event)

        # If we loaded fewer candidate paths than the total available (i.e. we
        # applied early stopping) *and* we didn't get enough matches, the hints
        # were too imprecise for this query — signal the caller to do a full
        # scan so we never return a truncated result set.
        loaded_all = len(candidate_paths) == len(path_hints)
        if not loaded_all and len(matching) < limit:
            return None

        return matching

    async def count_events(
        self,
        conversation_id: UUID,
        kind__eq: EventKind | None = None,
        timestamp__gte: datetime | None = None,
        timestamp__lt: datetime | None = None,
    ) -> int:
        """Count events matching the given filters."""
        # If we are not filtering, we can simply count the paths
        if not (kind__eq or timestamp__gte or timestamp__lt):
            conversation_path = await self.get_conversation_path(conversation_id)
            result = await self._count_events_no_filter(conversation_path)
            return result

        events = page_iterator(
            self.search_events,
            conversation_id=conversation_id,
            kind__eq=kind__eq,
            timestamp__gte=timestamp__gte,
            timestamp__lt=timestamp__lt,
        )
        result = 0
        async for event in events:
            result += 1
        return result

    async def _count_events_no_filter(self, conversation_path: Path) -> int:
        """Count all event files in the conversation directory without filtering."""
        loop = asyncio.get_running_loop()
        path_hints = await loop.run_in_executor(None, self._search_paths, conversation_path)
        return len(path_hints)

    async def save_event(self, conversation_id: UUID, event: Event):
        if isinstance(event.id, str):
            id_hex = event.id.replace('-', '')
        else:
            id_hex = event.id.hex  # type: ignore[unreachable]
        path = (await self.get_conversation_path(conversation_id)) / f'{id_hex}.json'
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._store_event, path, event)

    async def batch_get_events(
        self, conversation_id: UUID, event_ids: list[UUID]
    ) -> list[Event | None]:
        """Given a list of ids, get events (Or none for any which were not found)."""
        return await asyncio.gather(
            *[self.get_event(conversation_id, event_id) for event_id in event_ids]
        )
