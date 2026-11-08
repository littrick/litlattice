//! Port of `linebasedBufferedWriter` from `cmd/gitserver/internal/server.go`.
//!
//! The clone/fetch progress stream is teed into this buffer for
//! `SetLastOutput`. A `\r` rewinds the write offset to just after the last
//! newline, so intermediate `git --progress` reports overwrite each other
//! instead of piling up; stale bytes past the write offset stay readable until
//! they are overwritten, exactly like the baseline.

/// Port of `linebasedBufferedWriter`.
#[derive(Debug, Default)]
pub struct LinebasedWriter {
    buffer: Vec<u8>,
    /// Offset in `buffer` where the next write begins.
    write_offset: usize,
    /// Index just after the last `\n`, or 0 when there is none.
    after_last_newline: usize,
}

impl LinebasedWriter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append `data` with the baseline's `\r`/`\n` handling.
    pub fn write(&mut self, mut data: &[u8]) {
        while !data.is_empty() {
            let Some(index) = data
                .iter()
                .position(|byte| *byte == b'\r' || *byte == b'\n')
            else {
                // `append(w.buf[:w.writeOffset], p...)`: the append truncates
                // anything after the write offset.
                self.buffer.truncate(self.write_offset);
                self.buffer.extend_from_slice(data);
                self.write_offset = self.buffer.len();
                break;
            };

            self.buffer.truncate(self.write_offset);
            self.buffer.extend_from_slice(&data[..=index]);
            match data[index] {
                b'\n' => {
                    self.write_offset = self.buffer.len();
                    self.after_last_newline = self.buffer.len();
                }
                // Record that the next write should overwrite the data after
                // the most recent newline, but keep the stale tail readable.
                _ => self.write_offset = self.after_last_newline,
            }
            data = &data[index + 1..];
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer
    }

    pub fn to_string_lossy(&self) -> String {
        String::from_utf8_lossy(&self.buffer).into_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_all(parts: &[&[u8]]) -> LinebasedWriter {
        let mut writer = LinebasedWriter::new();
        for part in parts {
            writer.write(part);
        }
        writer
    }

    #[test]
    fn keeps_newline_terminated_lines() {
        let writer = write_all(&[b"one\ntwo\nthree"]);
        // The last line is unterminated but still visible.
        assert_eq!(writer.as_bytes(), b"one\ntwo\nthree");
    }

    #[test]
    fn progress_reporters_overwrite_the_current_line() {
        // Same stream as a `git --progress` transfer: each `\r` rewinds to
        // the start of the current line, so only the final report remains.
        let writer = write_all(&[
            b"Receiving objects: 1%\r",
            b"Receiving objects: 2%\r",
            b"Receiving objects: 100% (1/1)",
            b", done.\n",
        ]);
        assert_eq!(writer.as_bytes(), b"Receiving objects: 100% (1/1), done.\n");
    }

    #[test]
    fn keeps_stale_tail_until_it_is_overwritten() {
        let mut writer = write_all(&[b"abcdef\nshort\r"]);
        // `\r` rewinds but does not erase: the old tail is still reported.
        assert_eq!(writer.as_bytes(), b"abcdef\nshort\r");
        // The next write overwrites from just after the newline.
        writer.write(b"xy");
        assert_eq!(writer.as_bytes(), b"abcdef\nxy");
        // A following newline catches up with the new write offset.
        writer.write(b"z\n");
        assert_eq!(writer.as_bytes(), b"abcdef\nxyz\n");
    }

    #[test]
    fn multiple_lines_in_one_chunk() {
        let writer = write_all(&[b"a\nb\rcc\nd\n"]);
        assert_eq!(writer.as_bytes(), b"a\ncc\nd\n");
    }
}
