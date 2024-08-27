use ibverbs_rs::{sender::Sender, Family, IbvAccessFlags, IbvMr, IbvSendWr, IbvSge, IbvWcOpcode, IbvWrOpcode, LookUpBy, MrMetadata, SendRecv};
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

    let message = "Hello, Rust!";
    let message_bytes: Vec<u8> = message.as_bytes().to_vec();
    let message_bytes_ptr = message_bytes.as_ptr();
    let flags = IbvAccessFlags::LocalWrite.as_i32() | IbvAccessFlags::RemoteWrite.as_i32() | IbvAccessFlags::RemoteRead.as_i32();
    let mr = IbvMr::new(sender.pd(), message_bytes_ptr as *mut u8, message.len(), flags);
    sender.set_metadata_address(mr.addr());
    sender.set_metadata_rkey(mr.rkey());
    sender.set_metadata_length(message.len() as u64);

    let new_mr_metadata = MrMetadata{
        address: mr.addr(),
        rkey: mr.rkey(),
        length: message.len() as u64,
    };

    let metadata_mr = IbvMr::new(sender.pd(), &new_mr_metadata as *const MrMetadata as *mut u8, MrMetadata::SIZE, flags);
    let metadata_mr_addr = metadata_mr.addr();
    let metadata_mr_lkey = metadata_mr.lkey();
    info!("Sending metadata: {:#?}", new_mr_metadata);
    info!("to receiver address: {}, rkey {}", sender.receiver_metadata_address, sender.receiver_metadata_rkey);
    let sge = IbvSge::new(metadata_mr_addr, MrMetadata::SIZE as u32, metadata_mr_lkey);
    let send_wr = IbvSendWr::new(
        0,
        sge,
        1,
        IbvWrOpcode::RdmaWriteWithImm,
        flags,
        sender.receiver_metadata_address,
        sender.receiver_metadata_rkey,
    );
    sender.qp_list[0].ibv_post_send(send_wr)?;
    sender.qp_list[0].complete(1, IbvWcOpcode::RdmaWrite, SendRecv::Send)?;
    info!("Client: send posted");
    Ok(())
}