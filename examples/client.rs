use std::ffi::c_void;

use ibverbs_rs::{sender::Sender, Family, IbvAccessFlags, IbvMr, IbvRecvWr, IbvSendWr, IbvSendWrList, IbvWcOpcode, IbvWrOpcode, LookUpBy, MrMetadata, QpMode, SendRecv};
use clap::Parser;


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
    let num_qps = 2;
    let mut sender = Sender::new(
        LookUpBy::Name(device_name),
        server.parse().unwrap(),
        port,
        num_qps,
        Family::Inet,
        QpMode::Single,
    )?;
    sender.create_metadata()?;
    sender.connect()?;

    let message: [u8;65537*11] = [1;65537*11]; 
    let flags = IbvAccessFlags::LocalWrite.as_i32() | IbvAccessFlags::RemoteWrite.as_i32() | IbvAccessFlags::RemoteRead.as_i32();
    let addr = &message as *const u8 as *mut c_void;
    let message_mr = IbvMr::new(sender.pd(), addr, message.len(), flags);
    let new_mr_metadata = MrMetadata{
        address: message_mr.addr(),
        rkey: message_mr.rkey(),
        padding: 0,
        length: message.len() as u64,
    };
    let addr = &new_mr_metadata as *const MrMetadata as *mut c_void;
    let metadata_mr = IbvMr::new(sender.pd(), addr, MrMetadata::SIZE, flags);
    let send_wr = IbvSendWr::new(
        &metadata_mr,
        sender.receiver_metadata_address,
        sender.receiver_metadata_rkey,
        IbvWrOpcode::RdmaWriteWithImm,
    );
    sender.qp_list[0].ibv_post_send(send_wr.as_ptr())?;
    sender.qp_list[0].complete(1, IbvWcOpcode::RdmaWrite, SendRecv::Send)?;

    let notify_wr = IbvRecvWr::new(&sender.get_sender_metadata_mr());
    sender.qp_list[0].ibv_post_recv(notify_wr)?;
    sender.qp_list[0].complete(1, IbvWcOpcode::RecvRdmaWithImm, SendRecv::Recv)?;
    
    let recv_metadata = sender.get_sender_metadata();
    let mut send_wr_list = IbvSendWrList::new(
        &message_mr,
        recv_metadata.address,
        recv_metadata.rkey,
        num_qps as u64,
        IbvWrOpcode::RdmaWrite,
    );
    let mut jh_list = Vec::new();
    let mut idx = 0;
    while let Some(send_wr) = send_wr_list.pop() {
        let qp = sender.qp_list[idx].clone();
        let jh = std::thread::spawn(move || {
            qp.ibv_post_send(send_wr.as_ptr()).unwrap();
            qp.complete(1, IbvWcOpcode::RdmaWrite, SendRecv::Send).unwrap();
        });
        idx += 1;
        jh_list.push(jh);
    }
    for jh in jh_list {
        jh.join().unwrap();
    }
    let notify_wr = IbvSendWr::new(
        &metadata_mr,
        sender.receiver_metadata_address,
        sender.receiver_metadata_rkey,
        IbvWrOpcode::RdmaWriteWithImm,
    );
    sender.qp_list[0].ibv_post_send(notify_wr.as_ptr())?;
    sender.qp_list[0].complete(1, IbvWcOpcode::RdmaWrite, SendRecv::Send)?;

    Ok(())
}