use std::time::Duration;

use ibverbs_rs::{sender::Sender, Family, IbvAccessFlags, IbvMr, IbvSendWr, IbvSge, IbvWrOpcode, LookUpBy, MrMetadata};
use clap::Parser;
use log::info;

#[derive(Parser)]
pub struct Args{
    #[clap(short, long)]
    pub device_name: String,
    #[clap(short, long)]
    pub server: String,
    #[clap(short, long)]
    pub port: u16,
}

fn main() -> anyhow::Result<()> {
    ibverbs_rs::initialize_logger();
    let args = Args::parse();
    let device_name = args.device_name;
    let server = args.server;
    let port = args.port;

    let mut sender = Sender::new(
        LookUpBy::Name(device_name),
        server.parse().unwrap(),
        port,
        1,
        Family::Inet,
    )?;
    sender.connect()?;
    //std::thread::sleep(Duration::from_secs(5));

    let message = "Hello, Rust!";
    let message_bytes: Vec<u8> = message.as_bytes().to_vec();
    let message_bytes_ptr = message_bytes.as_ptr();

    info!("message length: {}", message.len());
    let flags = IbvAccessFlags::LocalWrite.as_i32() | IbvAccessFlags::RemoteWrite.as_i32() | IbvAccessFlags::RemoteRead.as_i32();
    info!("Client: creating memory region for message, flags: {}", flags);
    let mr = IbvMr::new(&sender.pd, message_bytes_ptr as *mut u8, message.len(), flags);
    info!("Client: mr addr: {}, rkey: {}", mr.addr(), mr.rkey());
    sender.set_metadata_address(mr.addr());
    sender.set_metadata_rkey(mr.rkey());
    sender.set_metadata_length(message.len() as u64);
    let sge = IbvSge::new(sender.metadata_addr(), MrMetadata::SIZE as u32, sender.metadata_lkey());
    let send_wr = IbvSendWr::new(
        0,
        sge,
        1,
        IbvWrOpcode::Send,
        flags,
        sender.receiver_metadata_address,
        sender.receiver_metadata_rkey,
    );
    info!("Client: posting send");
    sender.qp_list[0].ibv_post_send(send_wr)?;
    info!("Client: send posted");
    Ok(())
}