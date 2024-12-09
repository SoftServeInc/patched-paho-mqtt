import collections
from contextlib import suppress
import logging
import selectors
from typing import cast, Any, Literal

from greenlet import GreenletExit
import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackOnPublish_v1, CallbackOnPublish_v2, ReasonCode
from paho.mqtt.enums import (
    CallbackAPIVersion, MQTTErrorCode, MQTTProtocolVersion, _ConnectionState
)
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.client import MQTTv311, MQTT_LOG_ERR, PUBLISH, DISCONNECT, time_func, Properties

logger = logging.getLogger()


class PatchedPahoClient(mqtt.Client):

    def __init__(
            self,
            callback_api_version: CallbackAPIVersion = CallbackAPIVersion.VERSION1,
            client_id: str | None = "",
            clean_session: bool | None = None,
            userdata: Any = None,
            protocol: MQTTProtocolVersion = MQTTv311,
            transport: Literal["tcp", "websockets", "unix"] = "tcp",
            reconnect_on_failure: bool = True,
            manual_ack: bool = False
    ):
        super().__init__(
            callback_api_version, client_id, clean_session, userdata,
            protocol, transport, reconnect_on_failure, manual_ack
        )

        self._current_attempt = None
        self._reconnect_delay_base = None
        self._reconnect_attempts_max = None

        # Allows to force Paho stop activity if the trigger in parent thread is set
        self.stop_event = None

    def _loop(self, timeout: float = 1.0) -> MQTTErrorCode:
        if timeout < 0.0:
            raise ValueError("Invalid timeout.")

        # Here we replace select with selectors call to avoid max open descriptors
        # max open descriptors limit. This patch inspired by
        # https://github.com/eclipse-paho/paho.mqtt.python/issues/697
        sock_sel = selectors.DefaultSelector()

        eventmask = selectors.EVENT_READ

        with suppress(IndexError):
            packet = self._out_packet.popleft()
            self._out_packet.appendleft(packet)
            eventmask = selectors.EVENT_WRITE | eventmask

        pending_bytes = 0
        if hasattr(self._sock, "pending"):
            pending_bytes = self._sock.pending()

        # if bytes are pending do not wait in select
        if pending_bytes > 0:
            timeout = 0.0

        try:
            if self._sockpairR is None:
                sock_sel.register(self._sock, eventmask)
            else:
                sock_sel.register(self._sock, eventmask)
                sock_sel.register(self._sockpairR, selectors.EVENT_READ)

            events = sock_sel.select(timeout)
        except TypeError:
            # Socket isn't correct type, in likelihood connection is lost
            # ... or we called disconnect(). In that case the socket will
            # be closed but some loop (like loop_forever) will continue to
            # call _loop(). We still want to break that loop by returning an
            # rc != MQTT_ERR_SUCCESS and we don't want state to change from
            # mqtt_cs_disconnecting.
            if self._state not in (_ConnectionState.MQTT_CS_DISCONNECTING, _ConnectionState.MQTT_CS_DISCONNECTED):
                self._state = _ConnectionState.MQTT_CS_CONNECTION_LOST
            return MQTTErrorCode.MQTT_ERR_CONN_LOST
        except ValueError:
            # Can occur if we just reconnected but rlist/wlist contain a -1 for
            # some reason.
            if self._state not in (_ConnectionState.MQTT_CS_DISCONNECTING, _ConnectionState.MQTT_CS_DISCONNECTED):
                self._state = _ConnectionState.MQTT_CS_CONNECTION_LOST
            return MQTTErrorCode.MQTT_ERR_CONN_LOST
        except GreenletExit:
            # Greenlet finished, let's terminate the connection
            return MQTTErrorCode.MQTT_ERR_UNKNOWN
        except Exception:
            # Note that KeyboardInterrupt, etc. can still terminate since they
            # are not derived from Exception
            return MQTTErrorCode.MQTT_ERR_UNKNOWN

        socklist: list[list] = [[], []]

        for key, _event in events:
            if key.events & selectors.EVENT_READ:
                socklist[0].append(key.fileobj)

            if key.events & selectors.EVENT_WRITE:
                socklist[1].append(key.fileobj)

        if (self._sock in socklist[0] or pending_bytes > 0) and not self.stop_event.is_set():
            rc = self.loop_read()
            if rc or self._sock is None:
                return rc

        if self.stop_event.is_set():
            return self.use_circuit_breaker(sock_sel)

        if (self._sockpairR and self._sockpairR in socklist[0]) and not self.stop_event.is_set():
            # Stimulate output write even though we didn't ask for it, because
            # at that point publish or other command wasn't present.
            socklist[1].insert(0, self._sock)
            # Clear sockpairR - only ever a single byte written.
            with suppress(BlockingIOError):
                # Read many bytes at once - this allows up to 10000 calls to
                # publish() inbetween calls to loop().
                self._sockpairR.recv(10000)
        if self.stop_event.is_set():
            return self.use_circuit_breaker(sock_sel)

        if self._sock in socklist[1] and not self.stop_event.is_set():
            rc = self.loop_write()
            if rc or self._sock is None:
                return rc
        if self.stop_event.is_set():
            return self.use_circuit_breaker(sock_sel)

        sock_sel.close()

        return self.loop_misc()

    def use_circuit_breaker(self, socket_selector):
        socket_selector.close()
        self._state = _ConnectionState.MQTT_CS_DISCONNECTED
        self._reconnect_on_failure = False
        return MQTTErrorCode.MQTT_ERR_NOMEM

    def _packet_write(self) -> MQTTErrorCode:
        while True:
            if self.stop_event.is_set():
                self._handle_pubackcomp = lambda *args: MQTTErrorCode.MQTT_ERR_SUCCESS
                # empty in/out buffers
                with self._out_message_mutex:
                    self._out_messages = {}
                    self._out_packet = collections.deque()
                # set empty input buffers
                with self._in_message_mutex:
                    self._in_messages = {}
                    self._in_packet = {
                        "command": 0,
                        "have_remaining": 0,
                        "remaining_count": [],
                        "remaining_mult": 1,
                        "remaining_length": 0,
                        "packet": bytearray(b""),
                        "to_process": 0,
                        "pos": 0,
                    }
                break

            try:
                packet = self._out_packet.popleft()
            except IndexError:
                return MQTTErrorCode.MQTT_ERR_SUCCESS

            try:
                write_length = self._sock_send(
                    packet['packet'][packet['pos']:])
            except (AttributeError, ValueError):
                self._out_packet.appendleft(packet)
                return MQTTErrorCode.MQTT_ERR_SUCCESS
            except BlockingIOError:
                self._out_packet.appendleft(packet)
                return MQTTErrorCode.MQTT_ERR_AGAIN
            except OSError as err:
                self._out_packet.appendleft(packet)
                self._easy_log(
                    MQTT_LOG_ERR, 'failed to receive on socket: %s', err)
                return MQTTErrorCode.MQTT_ERR_CONN_LOST

            if write_length > 0 and not self.stop_event.is_set():
                packet['to_process'] -= write_length
                packet['pos'] += write_length

                if packet['to_process'] == 0:
                    if (packet['command'] & 0xF0) == PUBLISH and packet['qos'] == 0:
                        with self._callback_mutex:
                            on_publish = self.on_publish

                        if on_publish:
                            with self._in_callback_mutex:
                                try:
                                    if self._callback_api_version == CallbackAPIVersion.VERSION1:
                                        on_publish = cast(CallbackOnPublish_v1, on_publish)

                                        on_publish(self, self._userdata, packet["mid"])
                                    elif self._callback_api_version == CallbackAPIVersion.VERSION2:
                                        on_publish = cast(CallbackOnPublish_v2, on_publish)

                                        on_publish(
                                            self,
                                            self._userdata,
                                            packet["mid"],
                                            ReasonCode(PacketTypes.PUBACK),
                                            Properties(PacketTypes.PUBACK),
                                        )
                                    else:
                                        raise RuntimeError("Unsupported callback API version")
                                except Exception as err:
                                    self._easy_log(
                                        MQTT_LOG_ERR, 'Caught exception in on_publish: %s', err)
                                    if not self.suppress_exceptions:
                                        raise

                        # TODO: Something is odd here. I don't see why packet["info"] can't be None.
                        # A packet could be produced by _handle_connack with qos=0 and no info
                        # (around line 3645). Ignore the mypy check for now but I feel there is a bug
                        # somewhere.
                        packet['info']._set_as_published()  # type: ignore

                    if (packet['command'] & 0xF0) == DISCONNECT:
                        with self._msgtime_mutex:
                            self._last_msg_out = time_func()

                        self._do_on_disconnect(
                            packet_from_broker=False,
                            v1_rc=MQTTErrorCode.MQTT_ERR_SUCCESS,
                        )
                        self._sock_close()
                        # Only change to disconnected if the disconnection was wanted
                        # by the client (== state was disconnecting). If the broker disconnected
                        # use unilaterally don't change the state and client may reconnect.
                        if self._state == _ConnectionState.MQTT_CS_DISCONNECTING:
                            self._state = _ConnectionState.MQTT_CS_DISCONNECTED
                        return MQTTErrorCode.MQTT_ERR_SUCCESS

                else:
                    # We haven't finished with this packet
                    self._out_packet.appendleft(packet)
            else:
                break

        with self._msgtime_mutex:
            self._last_msg_out = time_func()

        return MQTTErrorCode.MQTT_ERR_SUCCESS
