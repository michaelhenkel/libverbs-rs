use clap::Parser;
use ibverbs_rs::{receiver::Receiver, Hints, IbvAccessFlags, IbvMr, IbvRecvWr, IbvSendWr, IbvSge, IbvWcOpcode, IbvWrOpcode, LookUpBy, Message, MrMetadata, SendRecv};
use log::info;

#[derive(Parser)]
pub struct Args{
    #[clap(short, long)]
    pub device_name: String,
    #[clap(short, long)]
    pub address: String,
    #[clap(short, long)]
    pub port: u16,
}

fn main() -> anyhow::Result<()> {
    ibverbs_rs::initialize_logger();
    let args = Args::parse();
    let device_name = args.device_name;
    let address = args.address;
    let port = args.port;

    info!("Starting receiver on device {} with address {} and port {}", device_name, address, port);
    let mut receiver = Receiver::new(
        LookUpBy::Name(device_name),
        port,
    )?;
    receiver.create_metadata_mr()?;
    let hints = Hints::Address(address.parse().unwrap());
    receiver.listen(hints)?;
    receiver.connect()?;
    receiver.qp_list[0].state()?;

    let sge = IbvSge::new(receiver.metadata_addr(), MrMetadata::SIZE as u32, receiver.metadata_lkey());
    let notify_wr = IbvRecvWr::new(0,sge,1);
    receiver.qp_list[0].ibv_post_recv(notify_wr)?;
    receiver.qp_list[0].complete(1, IbvWcOpcode::RecvRdmaWithImm, SendRecv::Recv)?;
    let msg_len = receiver.get_receiver_metadata().length;
    let buffer = Message{
        id: 0,
    };
    let flags = IbvAccessFlags::LocalWrite.as_i32() | IbvAccessFlags::RemoteWrite.as_i32() | IbvAccessFlags::RemoteRead.as_i32();
    let msg_mr = IbvMr::new(receiver.pd(), &buffer, msg_len as usize, flags);
    let new_mr_metadata = MrMetadata{
        address: msg_mr.addr(),
        rkey: msg_mr.rkey(),
        padding: 0,
        length: msg_len as u64,
    };
    let metadata_mr = IbvMr::new(receiver.pd(), &new_mr_metadata, MrMetadata::SIZE, flags);
    let sge = IbvSge::new(metadata_mr.addr(), MrMetadata::SIZE as u32, metadata_mr.lkey());
    let send_wr = IbvSendWr::new(
        0,
        sge,
        1,
        IbvWrOpcode::RdmaWriteWithImm,
        flags,
        receiver.sender_metadata_address,
        receiver.sender_metadata_rkey,
    );
    receiver.qp_list[0].ibv_post_send(send_wr)?;
    receiver.qp_list[0].complete(1, IbvWcOpcode::RdmaWrite, SendRecv::Send)?;
    let notify_sge = IbvSge::new(receiver.metadata_addr(), MrMetadata::SIZE as u32, receiver.metadata_lkey());
    let notify_wr = IbvRecvWr::new(0,notify_sge,1);
    receiver.qp_list[0].ibv_post_recv(notify_wr)?;
    receiver.qp_list[0].complete(1, IbvWcOpcode::RecvRdmaWithImm, SendRecv::Recv)?;
    info!("Buffer: {:#?}", buffer);
    Ok(())

}