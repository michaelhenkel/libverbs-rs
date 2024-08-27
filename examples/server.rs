use clap::Parser;
use ibverbs_rs::{receiver::Receiver, Hints, IbvAccessFlags, IbvMr, IbvRecvWr, IbvSendWr, IbvSge, LookUpBy, MrMetadata};
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
    let hints = Hints::Address(address.parse().unwrap());
    receiver.listen(hints)?;
    //let flags = IbvAccessFlags::LocalWrite.as_i32() | IbvAccessFlags::RemoteWrite.as_i32() | IbvAccessFlags::RemoteRead.as_i32();
    //let mr = IbvMr::new(&receiver.pd, buf.as_ptr() as *mut u8, buf.len(), flags as i32);
    let sge = IbvSge::new(receiver.metadata_addr(), MrMetadata::SIZE as u32, receiver.metadata_lkey());
    let notify_wr = IbvRecvWr::new(0,sge,1);
    info!("Posting receive");
    receiver.qp_list[0].ibv_post_recv(notify_wr)?;
    receiver.connect()?;
    info!("Receiver connected all QPs");
    receiver.qp_list[0].state()?;


    info!("Waiting for event");
    receiver.qp_list[0].wait_for_event()?;
    info!("Event received");
    Ok(())

}