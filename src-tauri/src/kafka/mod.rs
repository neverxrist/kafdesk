pub(crate) mod config;
mod admin_describe_groups;
mod admin_group_offsets;
mod admin_list_offsets;
mod consume;
mod consume_ctx;
mod decode;
mod dns_check;
mod group_snapshot;
mod groups;
mod produce;
mod session;
#[cfg(test)]
mod smoke_tests;
mod tail;

pub use session::KafkaSession;
