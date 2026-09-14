import csv
import importlib.util
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("summary", Path(__file__).resolve().parents[1] / "run/summarizeRun.py")
summary = importlib.util.module_from_spec(spec)
spec.loader.exec_module(summary)


class MeasurementWindowTest(unittest.TestCase):
    def test_window_percentiles_and_outcomes(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            with (directory / "result.csv").open("w", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(["run", "elapsed", "latency", "dblatency", "ttype", "rbk", "dskipped", "error"])
                writer.writerow([1, 119999, 999, 999, "NEW_ORDER", 0, 0, 0])
                for i in range(20):
                    writer.writerow([1, 120000 + i, i + 1, i + 1, "NEW_ORDER", 0, 0, 0])
                writer.writerow([1, 130000, 1, 1, "NEW_ORDER", 1, 0, 0])
                writer.writerow([1, 140000, 1, 1, "NEW_ORDER", 1, 0, 1])
                writer.writerow([1, 419999, 5, 5, "PAYMENT", 0, 0, 1])
                writer.writerow([1, 420000, 999, 999, "NEW_ORDER", 0, 0, 0])
            (directory / "connections.csv").write_text(
                "event,epoch_ms,handshakes,local_port,remote_address,terminal\n"
                "authenticated,999,1,1,127.0.0.1:4000,\n"
                "start,1000,1,0,,\n"
                "authenticated,120999,2,2,127.0.0.1:4000,benchmarksql-Term-01\n"
                "authenticated,121000,3,3,127.0.0.1:4000,benchmarksql-Term-01\n"
                "authenticated,200000,4,4,127.0.0.1:4000,\n"
                "authenticated,421000,5,5,127.0.0.1:4000,benchmarksql-Term-01\n"
                "end,421001,5,0,,\n", encoding="utf-8")
            result = summary.summarize(directory, 120, 300)
            self.assertTrue(result["measurement_valid"])
            self.assertEqual(result["tpmC"], 4)
            self.assertEqual(result["errors"], 2)
            self.assertEqual(result["intentional_rollbacks"], 1)
            self.assertEqual(result["reconnects"], 1)
            self.assertEqual(result["transactions"]["NEW_ORDER"]["p95_ms"], 19)
            self.assertIsNone(result["transactions"]["DELIVERY_BG"]["p95_ms"])
            (directory / "connections.csv").unlink()
            unmeasured = summary.summarize(directory, 120, 300)
            self.assertFalse(unmeasured["measurement_valid"])
            self.assertIsNone(unmeasured["reconnects"])

    def test_truncated_connection_measurement_is_not_zero(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "connections.csv"
            path.write_text("event,epoch_ms,handshakes,local_port,remote_address,terminal\n"
                            "authenticated,1,1,1,127.0.0.1:4000,\nstart,2,1,0,,\n", encoding="utf-8")
            count, problem = summary.connection_window(path, 120000, 420000)
            self.assertIsNone(count)
            self.assertIn("full window", problem)


if __name__ == "__main__":
    unittest.main()
