use ibverbs_rs::{sender::Sender, Family, IbvAccessFlags, IbvMr, IbvRecvWr, IbvSendWr, IbvSge, IbvWcOpcode, IbvWrOpcode, LookUpBy, Message, MrMetadata, SendRecv};
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
    sender.create_metadata()?;
    sender.connect()?;
    

    let message = Message{
        id: 666,
    };

    let flags = IbvAccessFlags::LocalWrite.as_i32() | IbvAccessFlags::RemoteWrite.as_i32() | IbvAccessFlags::RemoteRead.as_i32();
    let message_mr = IbvMr::new(sender.pd(), &message, message.len(), flags);
    let new_mr_metadata = MrMetadata{
        address: message_mr.addr(),
        rkey: message_mr.rkey(),
        padding: 0,
        length: message.len() as u64,
    };

    let metadata_mr = IbvMr::new(sender.pd(), &new_mr_metadata, MrMetadata::SIZE, flags);
    let sge = IbvSge::new(metadata_mr.addr(), MrMetadata::SIZE as u32, metadata_mr.lkey());
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
    let recv_sge = IbvSge::new(sender.metadata_addr(), MrMetadata::SIZE as u32, sender.metadata_lkey());
    let notify_wr = IbvRecvWr::new(0,recv_sge,1);
    sender.qp_list[0].ibv_post_recv(notify_wr)?;
    sender.qp_list[0].complete(1, IbvWcOpcode::RecvRdmaWithImm, SendRecv::Recv)?;
    let recv_metadata = sender.get_sender_metadata();
    let msg_sge = IbvSge::new(message_mr.addr(), message.len() as u32, message_mr.lkey());
    let send_wr = IbvSendWr::new(
        0,
        msg_sge,
        1,
        IbvWrOpcode::RdmaWrite,
        flags,
        recv_metadata.address,
        recv_metadata.rkey,
    );
    sender.qp_list[0].ibv_post_send(send_wr)?;
    sender.qp_list[0].complete(1, IbvWcOpcode::RdmaWrite, SendRecv::Send)?;

    let notify_sge = IbvSge::new(sender.metadata_addr(), MrMetadata::SIZE as u32, sender.metadata_lkey());
    let notify_wr = IbvSendWr::new(
        0,
        notify_sge,
        1,
        IbvWrOpcode::RdmaWriteWithImm,
        flags,
        sender.receiver_metadata_address,
        sender.receiver_metadata_rkey,
    );
    sender.qp_list[0].ibv_post_send(notify_wr)?;
    sender.qp_list[0].complete(1, IbvWcOpcode::RdmaWrite, SendRecv::Send)?;

    Ok(())
}