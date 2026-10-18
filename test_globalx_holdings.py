import json
import unittest

from globalx_holdings import read_full_holdings


def page(props, split=False):
    payload = '9a:' + json.dumps(["$", "dialog", None, props]) + '\n'
    chunks = [payload[:40], payload[40:]] if split else [payload]
    return ''.join('<script>self.__next_f.push(' + json.dumps([1, chunk]) + ')</script>' for chunk in chunks)


class GlobalXTests(unittest.TestCase):
    def props(self):
        return dict(ticker="MLPX", asOfDate="Oct 12, 2026", holdings=[dict(
            ticker="TRP", name="TC ENERGY CORP", netAssets="9.14%",
            shares="5,334,696", marketValue="$$329,364,131", sedol="BJMY6F9")])

    def test_real_schema_retains_provider_date_and_full_records(self):
        rows, date = read_full_holdings(page(self.props()), "MLPX")
        self.assertEqual(date, "2026-10-12")
        self.assertEqual(rows[0]["shares"], "5,334,696")

    def test_script_chunk_boundary(self):
        self.assertEqual(read_full_holdings(page(self.props(), split=True), "MLPX")[1], "2026-10-12")

    def test_other_fund_rejected(self):
        with self.assertRaises(ValueError):
            read_full_holdings(page(self.props()), "MLPA")

    def test_missing_date_never_uses_retrieval_date(self):
        props = self.props()
        del props["asOfDate"]
        with self.assertRaisesRegex(ValueError, "date missing"):
            read_full_holdings(page(props), "MLPX")

    def test_future_date_rejected(self):
        props = self.props()
        props["asOfDate"] = "Jan 01, 2099"
        with self.assertRaisesRegex(ValueError, "future"):
            read_full_holdings(page(props), "MLPX")

    def test_partial_records_rejected(self):
        props = self.props()
        del props["holdings"][0]["shares"]
        with self.assertRaisesRegex(ValueError, "incomplete"):
            read_full_holdings(page(props), "MLPX")

    def test_top_holdings_table_without_full_dialog_rejected(self):
        with self.assertRaises(ValueError):
            read_full_holdings('<table><tr><td>TRP</td></tr></table>', "MLPX")


if __name__ == "__main__":
    unittest.main()
