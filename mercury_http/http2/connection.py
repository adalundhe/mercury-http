
import asyncio
import time
import traceback
import h2.config
import h2.connection
import h2.events
import h2.exceptions
import h2.settings
from enum import IntEnum
from mercury_http.http.timeouts import Timeouts
from typing import AsyncIterator, Dict, List, Optional, Tuple
from .stream import AsyncStream
from .utils import has_body_headers
from .url import URL
from .response import HTTP2Response
from .request import HTTP2Request


class HTTPConnectionState(IntEnum):
    ACTIVE = 1
    IDLE = 2
    CLOSED = 3


class HTTP2Connection:
    READ_NUM_BYTES = 64 * 1024
    CONFIG = h2.config.H2Configuration(validate_inbound_headers=False)

    def __init__(self, timeouts: Timeouts, keepalive_expiry: Optional[float] = None, max_streams: int = 10**2):
        self._network_stream = AsyncStream(timeouts)
        self._keepalive_expiry: Optional[float] = keepalive_expiry
        self._h2_state = h2.connection.H2Connection(config=self.CONFIG)
        self._state = HTTPConnectionState.IDLE
        self._expire_at: Optional[float] = None
        self._used_all_stream_ids = False
        self._connection_error = False
        self._events: Dict[int, h2.events.Event] = {}
        self._read_exception: Optional[Exception] = None
        self._write_exception: Optional[Exception] = None
        self._connection_error_event: Optional[h2.events.Event] = None
        self._max_streams_semaphore = asyncio.Semaphore(max_streams)
        self._sent_connection_init = False
        self._connected = False
        self._request_name = None

    async def connect(self, request_name: str, url: URL):
        if self._connected is False or self._request_name != request_name:
            await self._network_stream.connect(url)
            self._connected = True
            self._request_name = request_name

    async def request(self, request_name: str, request: HTTP2Request) -> HTTP2Response:
        stream_id = 1

        try:
            start = time.time()

            if self._state in (HTTPConnectionState.ACTIVE, HTTPConnectionState.IDLE):
                self._state = HTTPConnectionState.ACTIVE

            if not self._sent_connection_init:
                await self._send_connection_init()
                self._sent_connection_init = True

            try: 
                stream_id = self._h2_state.get_next_available_stream_id()
                self._events[stream_id] = []
            except h2.exceptions.NoAvailableStreamIDError:
                self._used_all_stream_ids = True
                raise Exception('Connection unavailable.')
                
            self._events[stream_id] = []

            await self._send_request_headers(stream_id, request)
            await self._send_request_body(stream_id, request)
            status, response_headers = await self._receive_response(stream_id)

            elapsed = time.time() - start

            response = HTTP2Response(
                request_name,
                request
            )

            response.time = elapsed

            async for chunk in self._receive_response_body(stream_id):
                    response.content += chunk

            response.status = status
            response.headers = response_headers
            
            return response

        except Exception as e:
            return HTTP2Response(
                request_name, 
                request, 
                error=e
            )

    async def _send_connection_init(self) -> None:
        
        self._h2_state.local_settings = h2.settings.Settings(
            client=True,
            initial_values={
                h2.settings.SettingCodes.ENABLE_PUSH: 0,
                h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: 100,
                h2.settings.SettingCodes.MAX_HEADER_LIST_SIZE: 65536,
            },
        )

        del self._h2_state.local_settings[
            h2.settings.SettingCodes.ENABLE_CONNECT_PROTOCOL
        ]

        self._h2_state.initiate_connection()
        self._h2_state.increment_flow_control_window(2**24)
        await self._write_outgoing_data()

    async def _send_request_headers(self, stream_id: int, request: HTTP2Request) -> None:
        end_stream = not has_body_headers(request.headers)

        self._h2_state.send_headers(stream_id, request.headers, end_stream=end_stream)
        self._h2_state.increment_flow_control_window(2**24, stream_id=stream_id)
        await self._write_outgoing_data()

    async def _send_request_body(self, stream_id: int, request: HTTP2Request) -> None:
        if not has_body_headers(request.headers):
            return

        async for data in request:
            while data:
                local_flow = self._h2_state.local_flow_control_window(stream_id)
                max_frame_size = self._h2_state.max_outbound_frame_size
                flow = min(local_flow, max_frame_size)
                while flow == 0:
                    await self._receive_events()
                    local_flow = self._h2_state.local_flow_control_window(stream_id)
                    max_frame_size = self._h2_state.max_outbound_frame_size
                    flow = min(local_flow, max_frame_size)
                    
                max_flow = flow
                chunk_size = min(len(data), max_flow)
                chunk, data = data[:chunk_size], data[chunk_size:]
                self._h2_state.send_data(stream_id, chunk)
                await self._write_outgoing_data()

        self._h2_state.end_stream(stream_id)
        await self._write_outgoing_data()

    async def _receive_response(
        self, stream_id: int
    ) -> Tuple[int, List[Tuple[bytes, bytes]]]:

        event: h2.events.Event = None
        while True:
            event = await self._receive_stream_event(stream_id)
            if isinstance(event, h2.events.ResponseReceived):
                break

        status_code = 200
        headers = []

        if isinstance(event, h2.events.ResponseReceived):
            for k, v in event.headers:
                if k == b":status":
                    status_code = int(v.decode("ascii", errors="ignore"))
                elif not k.startswith(b":"):
                    headers.append((k, v))

        return (status_code, headers)

    async def _receive_response_body(
        self, stream_id: int
    ) -> AsyncIterator[bytes]:
        while True:
            event = await self._receive_stream_event(stream_id)
            if isinstance(event, h2.events.DataReceived):
                amount = event.flow_controlled_length
                self._h2_state.acknowledge_received_data(amount, stream_id)
                await self._write_outgoing_data()
                yield event.data
            elif isinstance(event, (h2.events.StreamEnded, h2.events.StreamReset)):
                break

    async def _receive_stream_event(
        self, stream_id: int
    ) -> h2.events.Event:

        while not self._events.get(stream_id):
            await self._receive_events(stream_id)
        event = self._events[stream_id].pop(0)

        if hasattr(event, "error_code"):
            raise Exception(str(event))
        return event

    async def _receive_events(
        self, stream_id: Optional[int] = None
    ) -> None:

        if stream_id is None or not self._events.get(stream_id):
            data = await self._network_stream.read()
            events = self._h2_state.receive_data(data)
            for event in events:
                event_stream_id = getattr(event, "stream_id", 0)

                if hasattr(event, "error_code") and event_stream_id == 0:
                    self._connection_error_event = event
                    raise Exception(str(event))

                if event_stream_id in self._events:
                    self._events[event_stream_id].append(event)

        await self._write_outgoing_data()

    async def _response_closed(self) -> None:
        self._h2_state.close_connection()
        self._state = HTTPConnectionState.CLOSED

    async def _write_outgoing_data(self) -> None:

        data_to_send = self._h2_state.data_to_send()
        if data_to_send == b'':
            return

        self._network_stream.write(data_to_send)

    def is_available(self) -> bool:
        return (
            self._state != HTTPConnectionState.CLOSED
            and not self._connection_error
            and not self._used_all_stream_ids
        )

    def has_expired(self) -> bool:
        now = time.monotonic()
        return self._expire_at is not None and now > self._expire_at

    def is_idle(self) -> bool:
        return self._state == HTTPConnectionState.IDLE

    def is_closed(self) -> bool:
        return self._state == HTTPConnectionState.CLOSED