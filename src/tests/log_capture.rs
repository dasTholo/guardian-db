//! Captures the tracing output of the current thread, so a test can assert what a
//! constructor logged (noosphere KB §12 "Nur-Lese-Kopie ohne Share-Ticket").

use std::io;
use std::sync::{Arc, Mutex};
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::fmt::MakeWriter;

#[derive(Clone, Default)]
pub struct LogCapture(Arc<Mutex<Vec<u8>>>);

impl LogCapture {
    /// Installs a capturing subscriber on this thread until the guard drops. The
    /// test must run on a current-thread runtime (the `#[tokio::test]` default).
    pub fn install() -> (Self, DefaultGuard) {
        let capture = Self::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(capture.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .finish();
        let guard = tracing::subscriber::set_default(subscriber);
        (capture, guard)
    }

    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}

pub struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

impl io::Write for CaptureWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for LogCapture {
    type Writer = CaptureWriter;
    fn make_writer(&'a self) -> Self::Writer {
        CaptureWriter(self.0.clone())
    }
}
