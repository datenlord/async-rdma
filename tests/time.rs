mod time {
    use clippy_utilities::Cast;
    use minstant::{Anchor, Instant};
    use std::time::{Duration, SystemTime};

    /// The Time Stamp Counter (TSC) is a 64-bit register present on all x86 processors since the
    /// Pentium, which is a high-resolution, low-overhead way for a program to get CPU timing information.
    ///
    /// `TSC` is faster if we just use `now()` and `elapsed()` to measure time in one machine.
    ///
    /// But in our scene, we need to transfer ddl between multiple machines, so we have to do
    /// some conversions and additions with Duration.
    ///
    /// SystemTime or clock_gettime (Realtime Clock) is faster in our scene.
    ///
    /// `minstant` is a drop-in replacement for `std::time::Instant` that measures time with high
    /// performance and high accuracy powered by TSC.
    #[allow(unused)] //#[test]
    fn main() {
        let timeout: Duration = Duration::new(1, 1);
        println!("minstant: {:?} ns", minstant(timeout));
        println!("systemtime: {:?} ns", systime(timeout));
    }

    fn minstant(timeout: Duration) -> u128 {
        let anchor = Anchor::new();
        let now = Instant::now();
        for _ in 0..1_000_000 {
            let _ddl = Instant::now()
                .as_unix_nanos(&anchor)
                .checked_add(timeout.as_nanos().cast())
                .unwrap();
        }
        now.elapsed().as_nanos() / 1_000_000
    }

    fn systime(timeout: Duration) -> u128 {
        let now = Instant::now();
        for _ in 0..1_000_000 {
            let _ddl = SystemTime::now().checked_add(timeout).unwrap();
        }
        now.elapsed().as_nanos() / 1_000_000
    }
}
