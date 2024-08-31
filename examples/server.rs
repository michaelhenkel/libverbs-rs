use std::ffi::c_void;

use clap::Parser;
use ibverbs_rs::{receiver::Receiver, Hints, IbvAccessFlags, IbvMr, IbvRecvWr, IbvSendWr, IbvWcOpcode, IbvWrOpcode, LookUpBy, MrMetadata, QpMode, SendRecv};
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
        QpMode::Single
    )?;
    receiver.create_metadata_mr()?;
    let hints = Hints::Address(address.parse().unwrap());
    receiver.listen(hints)?;
    receiver.accept()?;
    receiver.qp_list[0].state()?;

    let notify_wr = IbvRecvWr::new(&receiver.get_receiver_metadata_mr().unwrap());
    receiver.qp_list[0].ibv_post_recv(notify_wr)?;
    receiver.qp_list[0].complete(1, IbvWcOpcode::RecvRdmaWithImm, SendRecv::Recv)?;
    let msg_len = receiver.get_receiver_metadata().length;
    let buffer: [u8;65536*100] = [0;65536*100]; 
    let flags = IbvAccessFlags::LocalWrite.as_i32() | IbvAccessFlags::RemoteWrite.as_i32() | IbvAccessFlags::RemoteRead.as_i32();
    let addr = &buffer as *const u8 as *mut c_void;
    let msg_mr = IbvMr::new(receiver.pd(), addr, msg_len as usize, flags);
    let new_mr_metadata = MrMetadata{
        address: msg_mr.addr(),
        rkey: msg_mr.rkey(),
        padding: 0,
        length: msg_len as u64,
    };
    let addr = &new_mr_metadata as *const MrMetadata as *mut c_void;
    let metadata_mr = IbvMr::new(receiver.pd(), addr, MrMetadata::SIZE, flags);
    let send_wr = IbvSendWr::new(
        &metadata_mr,
        receiver.sender_metadata_address,
        receiver.sender_metadata_rkey,
        IbvWrOpcode::RdmaWriteWithImm,
    );
    receiver.qp_list[0].ibv_post_send(send_wr.as_ptr())?;
    receiver.qp_list[0].complete(1, IbvWcOpcode::RdmaWrite, SendRecv::Send)?;
    let notify_wr = IbvRecvWr::new(&receiver.get_receiver_metadata_mr().unwrap());
    receiver.qp_list[0].ibv_post_recv(notify_wr)?;
    receiver.qp_list[0].complete(1, IbvWcOpcode::RecvRdmaWithImm, SendRecv::Recv)?;
    // count amount of 1s in buffer
    let mut count = 0;
    for i in 0..msg_len as usize {
        if buffer[i] == 1 {
            count += 1;
        }
    }
    info!("Buffer 1: {:#?}", count);
    Ok(())

}