import os
import unittest
from unittest.mock import patch

os.environ["DISABLE_CACHE_SCHEDULER"] = "1"
os.environ["DISABLE_OAUTH_PREWARM"] = "1"

import app as app_module


class LoginRouteTests(unittest.TestCase):
    def setUp(self):
        self.client = app_module.app.test_client()

    def test_cache_scheduler_is_disabled_for_test_imports(self):
        self.assertIsNone(getattr(app_module.cache_scheduler, "_timer", None))

    def test_oauth_prewarm_is_disabled_for_test_imports(self):
        self.assertEqual(getattr(app_module.oauth.google, "server_metadata", None), {})

    def test_login_returns_error_page_when_oauth_redirect_fails(self):
        with patch.object(
            app_module.oauth.google,
            "authorize_redirect",
            side_effect=RuntimeError("oidc unavailable"),
        ):
            response = self.client.get("/login")

        self.assertEqual(response.status_code, 503)
        self.assertIn(b"Google sign-in is temporarily unavailable", response.data)

    def test_auth_callback_returns_error_page_when_token_exchange_fails(self):
        with patch.object(
            app_module.oauth.google,
            "authorize_access_token",
            side_effect=RuntimeError("token exchange failed"),
        ):
            response = self.client.get("/auth/callback")

        self.assertEqual(response.status_code, 503)
        self.assertIn(b"Google sign-in could not be completed", response.data)

    def test_auth_callback_returns_error_page_when_owner_lookup_fails(self):
        with patch.object(
            app_module.oauth.google,
            "authorize_access_token",
            return_value={"userinfo": {"email": "user@belfrysoftware.com"}},
        ), patch.object(
            app_module,
            "get_owners",
            side_effect=RuntimeError("hubspot owners unavailable"),
        ):
            response = self.client.get("/auth/callback")

        self.assertEqual(response.status_code, 503)
        self.assertIn(b"account access is being checked", response.data)

    def test_connect_rate_drivers_cold_cache_computes_live(self):
        with self.client.session_transaction() as sess:
            sess["authenticated"] = True

        payload = {
            "view": {"period": "this_month", "period_label": "This Month", "team": "all", "rep": "all", "rep_label": "All reps", "segment": "all", "segment_enabled": False, "is_rep_view": False},
            "filters": {"teams": [{"value": "all", "label": "All"}], "reps": [{"value": "all", "label": "All reps"}], "segments": []},
            "state": {"loading": False, "empty": False, "partial_explanation": False, "sample_too_small": False, "field_coverage_weak": False, "message": "Strong explanation"},
            "kpis": [],
            "notes": {
                "shared_number_definition": "Shared Number Rate flags the same normalized phone number appearing across multiple contact records, which is the closest read on reps calling the same number through different people.",
                "conversation_rate_definition": "Conversation rate uses the same definition as Call Stats: connected outbound calls with 60+ seconds duration divided by live connects.",
                "clearout_phone_source": "Current line-type and phone-quality logic uses HubSpot contact fields `cop_line_type`, `phone`, and `mobilephone`. No separate Clearout-specific field is wired into this page yet.",
            },
            "gap_decomposition": {"title": "What is driving the gap?", "expected_connect_pct": 10.1, "buckets": []},
            "driver_cards": [],
            "team_comparison": {"mode": "connect_pct", "modes": [], "team_avg_connect_pct": 10.1, "rows": []},
            "diagnostic_table": {"sort": "worst_delta_vs_team", "sorts": [], "rows": [], "team_avg_row": None},
            "rep_detail": {"selected_owner_id": None, "available": False},
        }

        with patch.object(
            app_module.analytics,
            "compute_connect_rate_drivers",
            return_value=payload,
        ):
            response = self.client.get("/calls/connect-rate-drivers")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Connect Rate Drivers", response.data)

    def test_connect_rate_drivers_page_renders_with_payload(self):
        with self.client.session_transaction() as sess:
            sess["authenticated"] = True

        payload = {
            "view": {
                "period": "this_month",
                "period_label": "This Month",
                "team": "all",
                "rep": "all",
                "rep_label": "All reps",
                "segment": "all",
                "segment_enabled": False,
                "is_rep_view": False,
            },
            "filters": {
                "teams": [{"value": "all", "label": "All"}],
                "reps": [{"value": "all", "label": "All reps"}],
                "segments": [],
            },
            "state": {
                "loading": False,
                "empty": False,
                "partial_explanation": False,
                "sample_too_small": False,
                "field_coverage_weak": False,
                "message": "Strong explanation",
            },
            "kpis": [
                {"label": "Selected Team Connect %", "display": "10.1%", "tip": None},
                {"label": "Team Avg Connect %", "display": "10.1%", "tip": None},
                {"label": "Delta vs Team Avg", "display": "0.0 pts", "tip": None},
                {"label": "Expected Connect %", "display": "10.1%", "tip": "Estimated connect rate based on dial mix, dialing behavior, and timing only."},
                {"label": "Actual vs Expected", "display": "0.0 pts", "tip": "Shows whether actual connect rate landed above or below the measured-condition benchmark."},
                {"label": "Gap Explained %", "display": "100%", "tip": "Shows how much of the gap versus team average is explained by the tracked drivers.", "band": "strong"},
                {"label": "Field Coverage %", "display": "85%", "tip": "Shows how much of the analyzed dialing volume has the fields needed to explain the read confidently."},
            ],
            "notes": {
                "shared_number_definition": "Shared Number Rate flags the same normalized phone number appearing across multiple contact records, which is the closest read on reps calling the same number through different people.",
                "conversation_rate_definition": "Conversation rate uses the same definition as Call Stats: connected outbound calls with 60+ seconds duration divided by live connects.",
                "clearout_phone_source": "Current line-type and phone-quality logic uses HubSpot contact fields `cop_line_type`, `phone`, and `mobilephone`. No separate Clearout-specific field is wired into this page yet.",
            },
            "gap_decomposition": {
                "title": "What is driving the gap?",
                "expected_connect_pct": 10.1,
                "buckets": [
                    {"label": "Dial Mix", "points": 0.0},
                    {"label": "Dialing Behavior", "points": 0.0},
                    {"label": "Timing", "points": 0.0},
                    {"label": "Unexplained", "points": 0.0},
                ],
            },
            "driver_cards": [
                {
                    "title": "Dial Mix",
                    "question": "Is this rep calling stronger or weaker reachable records than average?",
                    "index_label": "Dial Mix Index",
                    "index_value": 100,
                    "tip": "Composite read of reachable-record quality versus the selected team baseline.",
                    "rows": [],
                },
                {
                    "title": "Dialing Behavior",
                    "question": "Is this rep creating fresh reach efficiently or wasting volume?",
                    "index_label": "Reach Efficiency Index",
                    "index_value": 100,
                    "tip": "Composite read of how efficiently the dialing pattern creates fresh reach.",
                    "rows": [],
                },
                {
                    "title": "Timing",
                    "question": "Is this rep calling in productive windows?",
                    "index_label": "Timing Quality Index",
                    "index_value": 100,
                    "tip": "Composite read of timing quality versus when the selected team tends to connect best.",
                    "rows": [],
                },
            ],
            "team_comparison": {
                "mode": "connect_pct",
                "modes": [{"value": "connect_pct", "label": "Connect %"}],
                "team_avg_connect_pct": 10.1,
                "rows": [{"owner_id": "1", "rep": "Rep A", "actual_connect_pct": 10.1, "expected_connect_pct": 10.1, "delta_vs_team_avg": 0.0, "actual_vs_expected": 0.0, "selected": False}],
            },
            "diagnostic_table": {
                "sort": "worst_delta_vs_team",
                "sorts": [{"value": "worst_delta_vs_team", "label": "worst Delta vs Team"}],
                "rows": [{"owner_id": "1", "rep": "Rep A", "actual_connect_pct": 10.1, "delta_vs_team_avg": 0.0, "expected_connect_pct": 10.1, "actual_vs_expected": 0.0, "gap_explained_pct": 100.0, "shared_number_rate": 5.0, "conversation_pct": 60.0, "low_icp_rate": 10.0, "no_icp_data_rate": 0.0, "primary_driver": "Dial Mix", "secondary_driver": "Timing"}],
                "team_avg_row": {"rep": "Team Avg", "actual_connect_pct": 10.1, "expected_connect_pct": 10.1, "gap_explained_pct": 100.0, "shared_number_rate": 5.0, "conversation_pct": 60.0, "low_icp_rate": 10.0, "no_icp_data_rate": 0.0},
            },
            "rep_detail": {"selected_owner_id": None, "available": False},
        }

        with patch.object(app_module.calls_drilldown_bp, "is_cached", return_value=True), patch.object(
            app_module.analytics,
            "compute_connect_rate_drivers",
            return_value=payload,
        ):
            response = self.client.get("/calls/connect-rate-drivers")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Connect Rate Drivers", response.data)
        self.assertIn(b"Rep diagnostic table", response.data)
        self.assertIn(b"Same Number Across Contacts", response.data)
        self.assertNotIn(b"Rep Detail", response.data)

    def test_connect_rate_drivers_legacy_payload_is_normalized(self):
        with self.client.session_transaction() as sess:
            sess["authenticated"] = True

        legacy_payload = {
            "rows": [{
                "ae": "Rep A",
                "owner_id": "1",
                "connect_rate": 10.1,
                "vs_team": 0.0,
                "expected_connect_rate": 10.1,
                "actual_vs_expected": 0.0,
                "gap_explained_pct": 100.0,
                "unknown_line_pct": 5.0,
                "conversation_rate": 60.0,
                "icp_low_pct": 10.0,
                "icp_unknown_pct": 0.0,
                "dmi": 100.0,
                "rei": 100.0,
                "tqi": 100.0,
                "direct_line_pct": 25.0,
                "icp_high_pct": 31.4,
                "icp_mid_pct": 20.0,
                "peak_hour_pct": 50.0,
                "field_coverage_pct": 85.0,
            }],
            "team": {
                "connect_rate": 10.1,
                "avg_dials_per_day": 10.7,
                "icp_high_pct": 31.4,
                "direct_line_pct": 16.4,
                "voicemail_pct": 5.5,
                "convo_rate": 64.6,
                "peak_hour_pct": 50.0,
                "field_coverage_pct": 85.0,
            },
            "totals": {
                "connect_rate": 10.1,
                "conversation_rate": 64.6,
                "icp_low_pct": 0.0,
                "icp_unknown_pct": 43.9,
                "unknown_line_pct": 35.8,
            },
            "kpi_strip": {
                "Rep Connect %": 10.1,
                "Team Avg Connect %": 10.1,
                "Delta vs Team Avg": 0.0,
                "Expected Connect %": 10.1,
                "Actual vs Expected": 0.0,
                "Gap Explained %": 100.0,
                "Field Coverage %": 85.0,
            },
            "gap_decomposition": {
                "start": 10.1,
                "end": 10.1,
                "buckets": [{"label": "Dial Mix", "pts": 0.0}],
            },
            "driver_cards": {
                "dial_mix": {"index_value": 100.0, "rows": []},
                "dialing_behavior": {"index_value": 100.0, "rows": []},
                "timing": {"index_value": 100.0, "rows": []},
            },
            "diagnostic_rows": [{"owner_id": "1", "primary_driver": "Dial Mix", "secondary_driver": "Timing"}],
            "state_flags": {"is_empty": False, "partial_explanation": False, "low_coverage": False, "small_sample": False},
        }

        def legacy_compute(period, team="all", rep="all", segment="all"):
            return legacy_payload

        with patch.object(app_module.analytics, "compute_connect_rate_drivers", side_effect=legacy_compute):
            response = self.client.get("/calls/connect-rate-drivers")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Rep diagnostic table", response.data)
        self.assertIn(b"Convo %", response.data)


if __name__ == "__main__":
    unittest.main()
