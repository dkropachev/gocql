import json
import os
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cluster_bench_report  # noqa: E402
import cluster_cases  # noqa: E402

HERE = pathlib.Path(__file__).resolve().parent


def result(path, ops, errors=0):
    m = {'in_window': ops * 10, 'duration_seconds': 10, 'completed': ops * 10,
         'cpu_seconds_including_drain': 1.0, 'mallocs': ops * 100, 'alloc_bytes': ops * 1000,
         'errors': errors, 'latency': {'p90_us': 100, 'p95_us': 200, 'p99_us': 300}}
    path.write_text(json.dumps({'measurement': m}))


class CaseTable(unittest.TestCase):
    def test_shipped_table_matches_every_v2_case_once(self):
        groups = cluster_cases.load(HERE / 'cases.tsv')
        v2 = [c for g in groups for c in g[2]]
        self.assertEqual(len(v2), len(set(v2)))
        self.assertEqual(len(groups), 18)
        self.assertEqual(len(v2), 26)
        self.assertTrue(all(g[0].startswith('M') for g in groups))

    def test_mirrored_order(self):
        self.assertEqual(cluster_cases.order('MA3', ['A3', 'A4']),
                         [('A', 'MA3'), ('B', 'A3'), ('B', 'A4'), ('B', 'A4'), ('B', 'A3'), ('A', 'MA3')])

    def test_sequence_selects_and_repeats(self):
        groups = cluster_cases.select(cluster_cases.load(HERE / 'cases.tsv'), ['MF1'])
        runs = list(cluster_cases.sequence(groups, 2))
        self.assertEqual([r[2] for r in runs], [1, 1, 1, 1, 2, 2, 2, 2])
        self.assertEqual({r[1] for r in runs}, {64})

    def test_rejects_inconsistent_workers(self):
        with tempfile.TemporaryDirectory() as d:
            path = pathlib.Path(d) / 'c.tsv'
            path.write_text('MA1\tA1\t64\tx\nMA1\tA2\t4\ty\n')
            with self.assertRaises(ValueError):
                cluster_cases.load(path)

    def test_unknown_selection(self):
        with self.assertRaises(ValueError):
            cluster_cases.select(cluster_cases.load(HERE / 'cases.tsv'), ['MZ9'])

    def test_rate_uses_master_median(self):
        with tempfile.TemporaryDirectory() as d:
            out = pathlib.Path(d)
            for pair, ops in ((1, 100), (2, 300), (3, 200)):
                result(out / f'throughput-MA2-pair{pair}-MA2.json', ops)
            result(out / 'throughput-MA2-pair1-A2.json', 10000)
            self.assertEqual(cluster_cases.rate(d, 'MA2', 0.5), 100.0)


class Report(unittest.TestCase):
    def test_groups_v2_cases_under_master(self):
        with tempfile.TemporaryDirectory() as d:
            out = pathlib.Path(d)
            result(out / 'throughput-MA3-pair1-MA3.json', 100)
            result(out / 'throughput-MA3-pair1-A3.json', 50)
            result(out / 'throughput-MA3-pair1-A4.json', 200, errors=1)
            result(out / 'latency-MA3-pair1-MA3.json', 70)
            text = cluster_bench_report.report(d)
            self.assertIn('#### throughput MA3', text)
            self.assertIn('#### latency MA3', text)
            self.assertIn('| ops | 100.0 | 50.0 | 0.500 | 200.0 | 2.000 |', text)
            self.assertIn('| errors |', text)
            self.assertLess(text.index('throughput'), text.index('latency'))


if __name__ == '__main__':
    unittest.main()
