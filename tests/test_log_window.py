from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from framework_shells.log_projection import RecordProjection, project_record
from framework_shells.log_window import IndexCache, LineIndex, mark_log_reset, serialized_window


class LogWindowTests(unittest.TestCase):
    def test_source_mutation_before_response_is_rejected(self) -> None:
        for mutation in ("replace", "reset", "append"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "stdout"
                _ = path.write_bytes(b"old\n")
                index = LineIndex(path)
                try:
                    record = project_record(b"old\n", index.window().records[0].raw)

                    def mutate() -> None:
                        if mutation == "replace":
                            replacement = path.with_suffix(".new")
                            _ = replacement.write_bytes(b"new\n")
                            _ = replacement.replace(path)
                        elif mutation == "reset":
                            mark_log_reset(path)
                        else:
                            with path.open("ab") as writer:
                                _ = writer.write(b"new\n")

                    # The mutation occurs after bytes were read but before the response is returned.
                    def project_after_mutation(*_args: object, **_kwargs: object) -> RecordProjection:
                        mutate()
                        return record

                    with patch("framework_shells.log_window.project_record", return_value=record) as projector:
                        projector.side_effect = project_after_mutation
                        if mutation == "append":
                            self.assertEqual(index.window().records[0].text, "old\n")
                        else:
                            with self.assertRaisesRegex(RuntimeError, "changed"):
                                _ = index.window()
                    self.assertEqual(index.window().total, 2 if mutation == "append" else 1)
                finally:
                    index.close()

    def test_byte_limited_navigation_has_no_gaps(self) -> None:
        for codec in ("text", "messagepack"):
            with self.subTest(codec=codec), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "stdout"
                frame = b"line\n" if codec == "text" else bytes.fromhex("81a2696407")
                _ = path.write_bytes(frame * 40)
                index = LineIndex(path, codec=codec)
                try:
                    tail = index.window(max_bytes=1024)
                    current = index.window(action="current", current=tail.start, max_bytes=1024)
                    self.assertEqual((current.start, current.end), (tail.start, tail.end))
                    page = tail
                    seen = len(page.records)
                    while not page.at_start:
                        older = index.window(action="older", current=page.start, max_bytes=1024)
                        self.assertEqual(older.end, page.start)
                        self.assertLess(older.start, page.start)
                        seen += len(older.records)
                        page = older
                    self.assertEqual(seen, 40)
                    seen = len(page.records)
                    while not page.at_tail:
                        newer = index.window(action="newer", current=page.end, max_bytes=1024)
                        self.assertEqual(newer.start, page.end)
                        self.assertGreater(newer.end, page.end)
                        seen += len(newer.records)
                        page = newer
                    self.assertEqual(seen, 40)
                finally:
                    index.close()

    def test_cache_eviction_reset_and_close(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            paths = [Path(directory) / str(i) for i in range(3)]
            for path in paths:
                _ = path.write_bytes(b"first\n")
            cache = IndexCache(2)
            try:
                with cache.borrow(paths[0]) as index:
                    reference = index.window().records[0].raw
                with cache.borrow(paths[1]) as index:
                    evicted = index.window().records[0].raw
                with cache.borrow(paths[0]) as index:
                    self.assertEqual(index.raw(reference), b"first\n")
                with cache.borrow(paths[2]):
                    pass
                with cache.borrow(paths[0]) as index:
                    self.assertEqual(index.raw(reference), b"first\n")
                with cache.borrow(paths[1]) as index:
                    with self.assertRaisesRegex(ValueError, "stale"):
                        _ = index.raw(evicted)
                cache.invalidate(paths[0])
                with cache.borrow(paths[0]) as index:
                    with self.assertRaisesRegex(ValueError, "stale"):
                        _ = index.raw(reference)
            finally:
                cache.close()
            cache.close()
            with self.assertRaisesRegex(RuntimeError, "closed"):
                with cache.borrow(paths[0]):
                    pass
            self.assertEqual(paths[0].read_bytes(), b"first\n")
        with self.assertRaises(ValueError):
            _ = IndexCache(0)

    def test_reset_invalidates_all_codecs_without_file_change(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "stdout"
            _ = path.write_bytes(b'{}\n')
            cache = IndexCache()
            try:
                with cache.borrow(path, "text") as index:
                    text_ref = index.window().records[0].raw
                with cache.borrow(path, "json") as index:
                    json_ref = index.window().records[0].raw
                cache.invalidate(path)
                with cache.borrow(path, "text") as index:
                    with self.assertRaisesRegex(ValueError, "stale"):
                        _ = index.raw(text_ref)
                with cache.borrow(path, "json") as index:
                    with self.assertRaisesRegex(ValueError, "stale"):
                        _ = index.raw(json_ref)
            finally:
                cache.close()

    def test_messagepack_partial_append_and_raw(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "stdout"
            frame = bytes.fromhex("81a2696407")
            _ = path.write_bytes(frame + frame[:2])
            index = LineIndex(path, codec="messagepack")
            try:
                window = index.window()
                self.assertEqual(window.total, 1)
                self.assertEqual(window.pending_bytes, 2)
                self.assertEqual(window.records[0].text, '{"id":7}')
                self.assertEqual(index.raw(window.records[0].raw), frame)
                with path.open("ab") as writer:
                    _ = writer.write(frame[2:])
                next_window = index.window(generation=window.generation)
                self.assertEqual(next_window.total, 2)
                self.assertEqual(next_window.pending_bytes, 0)
                self.assertEqual(next_window.records[1].raw.byte_start, len(frame))
            finally:
                index.close()

    def test_corrupt_messagepack_never_becomes_partial_success(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "stdout"
            _ = path.write_bytes(bytes.fromhex("81a2696407c1"))
            index = LineIndex(path, codec="messagepack")
            try:
                for _ in range(2):
                    with self.assertRaises(ValueError):
                        _ = index.window()
            finally:
                index.close()

    def test_append_partial_raw_and_reset(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "stdout"
            _ = path.write_bytes(b"one\r\ntwo\npart")
            index = LineIndex(path)
            try:
                first = index.window(count=2, shift=1)
                self.assertEqual([r.text for r in first.records], ["two\n", "part"])
                self.assertEqual((first.start, first.end, first.total), (1, 3, 3))
                reference = first.records[1].raw
                self.assertEqual(index.raw(reference, offset=1, limit=2), b"ar")
                with path.open("ab") as writer:
                    _ = writer.write(b"ial\nlast\n")
                next_window = index.window(count=2, shift=1, generation=first.generation)
                self.assertEqual([r.text for r in next_window.records], ["partial\n", "last\n"])
                self.assertEqual(next_window.records[0].raw.byte_start, reference.byte_start)
                older = index.window(action="older", current=next_window.start, count=2, shift=1)
                self.assertEqual([r.text for r in older.records], ["two\n"])
                _ = path.write_bytes(b"new\n")
                with self.assertRaisesRegex(ValueError, "stale"):
                    _ = index.raw(reference)
                self.assertEqual(index.window().records[0].text, "new\n")
            finally:
                index.close()

    def test_whole_response_budget_and_empty(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "stdout"
            _ = path.write_bytes(b"")
            index = LineIndex(path)
            try:
                empty = index.window()
                self.assertEqual(empty.total, 0)
                self.assertTrue(empty.at_start and empty.at_tail)
                _ = path.write_bytes(b"line\n" * 1000)
                tail = index.window(max_bytes=1024)
                self.assertLessEqual(len(serialized_window(tail)), 1024)
                self.assertEqual(tail.end, 1000)
                self.assertGreater(tail.start, 0)
                current = index.window(action="current", count=1000, max_bytes=1024)
                self.assertEqual(current.start, 0)
                self.assertLess(current.end, 1000)
            finally:
                index.close()

    def test_giant_line_and_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "stdout"
            original = b"x" * (2 * 1024 * 1024) + b"\n"
            _ = path.write_bytes(original)
            index = LineIndex(path)
            try:
                window = index.window(codec="json")
                record = window.records[0]
                self.assertEqual(record.diagnostic, "parse_budget_exceeded")
                self.assertLessEqual(len(record.text.encode()), 8192)
                self.assertEqual(index.raw(record.raw, offset=len(original)-3), b"xx\n")
                self.assertEqual(path.read_bytes(), original)
                replacement = path.with_suffix(".new")
                _ = replacement.write_bytes(b"replacement")
                _ = replacement.replace(path)
                with self.assertRaisesRegex(ValueError, "stale"):
                    _ = index.window(generation=window.generation)
            finally:
                index.close()


if __name__ == "__main__":
    unittest.main()
