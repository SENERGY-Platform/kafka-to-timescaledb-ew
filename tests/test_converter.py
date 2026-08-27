"""
   Copyright 2026 InfAI (CC SES)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import datetime
import importlib.util
import os
import unittest

# loaded directly because the converter only needs the standard library, unlike
# the ew package, which pulls in kafka and database dependencies
_spec = importlib.util.spec_from_file_location(
    "ew_converter",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ew", "converter.py")
)
converter = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(converter)

ISO_FMT = "%Y-%m-%dT%H:%M:%S.%fZ"
EXPORT = "export-1"


class TestToDatetime(unittest.TestCase):
    def setUp(self):
        getattr(converter, "_learned_formats").clear()
        getattr(converter, "_logged_formats").clear()

    def test_configured_format(self):
        self.assertEqual(
            converter.to_datetime("2022-03-01T15:45:01.123000Z", ISO_FMT, EXPORT),
            datetime.datetime(2022, 3, 1, 15, 45, 1, 123000)
        )

    def test_configured_format_unix(self):
        self.assertEqual(
            converter.to_datetime(1646149501, "unix", EXPORT),
            datetime.datetime(2022, 3, 1, 15, 45, 1)
        )

    def test_none(self):
        self.assertIsNone(converter.to_datetime(None, ISO_FMT, EXPORT))

    def test_fallback_formats(self):
        expected = datetime.datetime(2022, 3, 1, 15, 45, 1)
        for value in (
            "2022-03-01T15:45:01Z",
            "2022-03-01T15:45:01",
            "2022-03-01T15:45:01+00:00",
            "2022-03-01T17:45:01+02:00",
            "2022-03-01T15:45:01.000000",
            "2022-03-01 15:45:01",
            "2022-03-01 15:45:01.000",
            "2022-03-01 15:45:01+00:00",
            "01.03.2022 15:45:01",
            "20220301T154501Z",
            "20220301154501",
            "Tue, 01 Mar 2022 15:45:01 +0000",
            "Tue, 01 Mar 2022 15:45:01 UTC",
            "Tuesday, 01-Mar-22 15:45:01 UTC",
            "2022-03-01T15:45:01.000000000Z",
            "2022-03-01T15:45:01.0000005Z",
            1646149501,
            1646149501.0,
            "1646149501",
            "1646149501000",
            "1646149501000000",
            "1646149501000000000",
        ):
            with self.subTest(value=value):
                getattr(converter, "_learned_formats").clear()
                self.assertEqual(converter.to_datetime(value, ISO_FMT, EXPORT), expected)

    def test_fallback_date_only(self):
        for value in ("2022-03-01", "01.03.2022", "20220301"):
            with self.subTest(value=value):
                getattr(converter, "_learned_formats").clear()
                self.assertEqual(converter.to_datetime(value, ISO_FMT, EXPORT), datetime.datetime(2022, 3, 1))

    def test_fallback_without_configured_format(self):
        self.assertEqual(
            converter.to_datetime("2022-03-01T15:45:01Z", None, EXPORT),
            datetime.datetime(2022, 3, 1, 15, 45, 1)
        )

    def test_compact_datetime_not_guessed_as_epoch(self):
        self.assertEqual(converter.to_datetime("20220301154501", ISO_FMT, EXPORT), datetime.datetime(2022, 3, 1, 15, 45, 1))

    def test_learned_format_is_reused(self):
        converter.to_datetime("2022-03-01 15:45:01", ISO_FMT, EXPORT)
        self.assertEqual(getattr(converter, "_learned_formats")[EXPORT], "%Y-%m-%d %H:%M:%S")
        self.assertEqual(
            converter.to_datetime("2022-03-02 15:45:01", ISO_FMT, EXPORT),
            datetime.datetime(2022, 3, 2, 15, 45, 1)
        )

    def test_learned_format_does_not_shadow_others(self):
        converter.to_datetime("2022-03-01 15:45:01", ISO_FMT, EXPORT)
        self.assertEqual(
            converter.to_datetime("2022-03-01T15:45:01.123000Z", ISO_FMT, EXPORT),
            datetime.datetime(2022, 3, 1, 15, 45, 1, 123000)
        )

    def test_exports_sharing_a_configured_format_do_not_evict_each_other(self):
        # both exports are configured with the same format but carry different
        # values, the learned format has to stay per export
        for _ in range(3):
            self.assertEqual(
                converter.to_datetime("2022-03-01 15:45:01", ISO_FMT, "export-a"),
                datetime.datetime(2022, 3, 1, 15, 45, 1)
            )
            self.assertEqual(
                converter.to_datetime("1646149501000", ISO_FMT, "export-b"),
                datetime.datetime(2022, 3, 1, 15, 45, 1)
            )
        learned = getattr(converter, "_learned_formats")
        self.assertEqual(learned["export-a"], "%Y-%m-%d %H:%M:%S")
        self.assertEqual(learned["export-b"], "unix")

    def test_unparsable(self):
        for value in ("", "not a time", "2022-13-45T99:99:99Z", {}, [1, 2]):
            with self.subTest(value=value):
                self.assertRaises(converter.TimeParseError, converter.to_datetime, value, ISO_FMT, EXPORT)


if __name__ == "__main__":
    unittest.main()
