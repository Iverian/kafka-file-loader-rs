use std::time::Duration;

pub const MAX_SEND_TIMEOUT: Duration = Duration::from_millis(10000);
pub const BASE_SEND_TIMEOUT: Duration = Duration::from_millis(500);
pub const SEND_TIMEOUT_MULTIPLIER: u32 = 2;
pub const BROADCAST_CAPACITY: usize = 4;
pub const RESEND_TIMEOUT: Duration = Duration::from_millis(400);
pub const RESEND_BATCH: usize = 50;
