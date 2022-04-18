
import asyncio
import time
import traceback
import h2.config
import h2.connection
import h2.events
import h2.exceptions
import h2.settings
from enum import IntEnum
from mercury_http.common.timeouts import Timeouts
from typing import Optional
from .stream import AsyncStream
from mercury_http.common import Response, Request


class HTTPConnectionState(IntEnum):
    ACTIVE = 1
    IDLE = 2
    CLOSED = 3


class HTTP2Connection:
    READ_NUM_BYTES = 64 * 1024
    MAX_WINDOW_SIZE = (2**24) - 1
    NEXT_WINDOW_SIZE = 2**24
    CONFIG = h2.config.H2Configuration(validate_inbound_headers=False)

    def __init__(self, timeouts: Timeouts, keepalive_expiry: Optional[float] = None, reset_connection: bool = False):
        self._network_stream = AsyncStream(timeouts)
        self._timeouts = timeouts
        self._keepalive_expiry: Optional[float] = keepalive_expiry
        self._h2_state = h2.connection.H2Connection(config=self.CONFIG)
        self._state = HTTPConnectionState.IDLE
        self._expire_at: Optional[float] = None
        self._used_all_stream_ids = False
        self._request_name = None
        self.connected = False
        self.reset_connection = reset_connection

    async def connect(self, request: Request):
        if self.connected is False or self._request_name != request.name or self.reset_connection:
            await self._network_stream.connect(request)
            self._request_name = request.name
            self._connected = True

            connection_init_data = self._prepare_connection_init()
            self._network_stream.write(connection_init_data)

    def get_stream_id(self) -> Response:

        self._state = HTTPConnectionState.ACTIVE

        try: 
            stream_id = self._h2_state.get_next_available_stream_id()

            return stream_id
        except h2.exceptions.NoAvailableStreamIDError:
            self._used_all_stream_ids = True
            raise Exception('Connection unavailable.')
            
    async def receive_response(self, stream_id: int, response: Response):
        
        read_events = True

        while read_events:
            async for event in self._receive_events():

                if hasattr(event, "error_code"):
                    raise Exception(str(event))

                elif isinstance(event, h2.events.ResponseReceived):
                    for k, v in event.headers:
                        if k == b":status":
                            status_code = int(v.decode("ascii", errors="ignore"))
                            response.response_code = status_code
                        elif not k.startswith(b":"):
                            response.headers[k] = v

                elif isinstance(event, h2.events.DataReceived):
                    amount = event.flow_controlled_length
                    self._h2_state.acknowledge_received_data(amount, stream_id)
                    self._write_outgoing_data()
                    response.body += event.data

                elif isinstance(event, (h2.events.StreamEnded, h2.events.StreamReset)) or event == b'':
                    read_events = False
                    break
        
        return response

    async def close(self) -> None:
        self._h2_state.close_connection()
        self._state = HTTPConnectionState.CLOSED

    def _prepare_connection_init(self) -> None:
        
        self._h2_state.local_settings = h2.settings.Settings(
            client=True,
            initial_values={
                h2.settings.SettingCodes.ENABLE_PUSH: 0,
                h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: 10000,
                h2.settings.SettingCodes.MAX_HEADER_LIST_SIZE: 65536,
            },
        )

        del self._h2_state.local_settings[
            h2.settings.SettingCodes.ENABLE_CONNECT_PROTOCOL
        ]

        self._h2_state.initiate_connection()
        self._h2_state.increment_flow_control_window(2**24)
        return self._h2_state.data_to_send()

    def send_request_headers(self, stream_id: int, request: Request) -> None:
        end_stream = request.payload.has_data is False

        self._h2_state.send_headers(stream_id, request.headers.encoded_headers, end_stream=end_stream)
        self._h2_state.increment_flow_control_window(2**24, stream_id=stream_id)
        headers = self._h2_state.data_to_send()
        self._network_stream.write(headers)
        
    async def submit_request_body(self, stream_id: int, request: Request) -> None:
        data = request.payload.encoded_data
        
        while data:
            local_flow = self._h2_state.local_flow_control_window(stream_id)
            max_frame_size = self._h2_state.max_outbound_frame_size
            flow = min(local_flow, max_frame_size)
            while flow == 0:
                [event async for event in self._receive_events()]
                local_flow = self._h2_state.local_flow_control_window(stream_id)
                max_frame_size = self._h2_state.max_outbound_frame_size
                flow = min(local_flow, max_frame_size)
                
            max_flow = flow
            chunk_size = min(len(data), max_flow)
            chunk, data = data[:chunk_size], data[chunk_size:]
            self._h2_state.send_data(stream_id, chunk)
            self._write_outgoing_data()

        self._h2_state.end_stream(stream_id)
        self._write_outgoing_data()

    async def _receive_events(self) -> None:
        # while not self._events.get(stream_id):
        data = await self._network_stream.read()
        if data == b'':
            yield data

        for event in self._h2_state.receive_data(data):
            yield event

        self._write_outgoing_data()

    def _write_outgoing_data(self) -> None:
        data_to_send = self._h2_state.data_to_send()
        if data_to_send == b'':
            return

        self._network_stream.write(data_to_send)
