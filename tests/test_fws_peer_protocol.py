from __future__ import annotations

import unittest
from collections.abc import Mapping
from typing import cast

from framework_shells.protocols.fws_peer import (
    FWS_NOTIFICATION_EVENT,
    FWS_PEER_NOTIFICATION_EVENT,
    FWS_PEER_REQUEST_EVENT,
    FWS_PEER_ROLE,
    FWS_PEER_SUBSCRIPTIONS_EVENT,
    FWS_SOCKETIO_NAMESPACE,
    FWS_SOCKETIO_SOCKET_PATH,
    build_peer_auth,
    build_peer_error_response,
    build_peer_shell_input_request,
    build_peer_subscriptions,
    build_peer_success_response,
    notification_shell_id,
    parse_peer_notification,
    parse_peer_shell_input_request,
    parse_peer_subscriptions_payload,
    peer_notification_requires_subscription,
)


class FwsPeerProtocolTests(unittest.TestCase):
    def test_socketio_binding_matches_existing_lane(self) -> None:
        self.assertEqual(FWS_SOCKETIO_NAMESPACE, "/fws")
        self.assertEqual(FWS_SOCKETIO_SOCKET_PATH, "/fws_ws/socket.io")
        self.assertEqual(FWS_PEER_SUBSCRIPTIONS_EVENT, "fws_peer_subscriptions")
        self.assertEqual(FWS_PEER_REQUEST_EVENT, "fws_peer_request")
        self.assertEqual(FWS_PEER_NOTIFICATION_EVENT, "fws_peer_notification")
        self.assertEqual(FWS_NOTIFICATION_EVENT, "fws_notification")

    def test_peer_auth_matches_existing_shape(self) -> None:
        self.assertEqual(
            build_peer_auth(api_token="token", runtime_id="runtime", pid="123"),
            {
                "role": FWS_PEER_ROLE,
                "api_token": "token",
                "runtime_id": "runtime",
                "pid": "123",
            },
        )

    def test_peer_shell_input_request_round_trips(self) -> None:
        request = build_peer_shell_input_request(
            shell_id="fs_1",
            data="payload",
            append_newline=True,
            eof=False,
            source="dashboard",
        )
        self.assertEqual(
            request,
            {
                "method": "fws.shell.input",
                "params": {
                    "shell_id": "fs_1",
                    "data": "payload",
                    "append_newline": True,
                    "eof": False,
                    "source": "dashboard",
                },
            },
        )
        self.assertEqual(parse_peer_shell_input_request(request), request)

    def test_peer_response_shapes_match_existing_ack_contract(self) -> None:
        self.assertEqual(build_peer_success_response({"accepted": True}), {"ok": True, "data": {"accepted": True}})
        self.assertEqual(build_peer_success_response(), {"ok": True})
        self.assertEqual(
            build_peer_error_response(code="not_owner", error="not mine"),
            {"ok": False, "code": "not_owner", "error": "not mine"},
        )

    def test_subscription_payload_is_normalized(self) -> None:
        payload = parse_peer_subscriptions_payload({"shell_ids": [" fs_1 ", "", 2]})
        self.assertEqual(payload, {"shell_ids": ["fs_1", "2"]})
        self.assertEqual(build_peer_subscriptions(["fs_1"]), {"shell_ids": ["fs_1"]})

    def test_peer_notification_filter_and_shell_id(self) -> None:
        notification = cast(
            Mapping[str, object],
            {
                "jsonrpc": "2.0",
                "method": "fws.logs.chunk",
                "params": {"shell_id": "fs_1", "stream": "stdout", "chunk": "x"},
            },
        )
        parsed = parse_peer_notification(notification)
        self.assertIsNotNone(parsed)
        self.assertEqual(notification_shell_id(notification), "fs_1")
        self.assertTrue(peer_notification_requires_subscription("fws.logs.chunk"))
        self.assertFalse(peer_notification_requires_subscription("fws.shell.updated"))


if __name__ == "__main__":
    unittest.main()
