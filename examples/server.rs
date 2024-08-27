use clap::Parser;
use ibverbs_rs::{receiver::Receiver, Hints, IbvRecvWr, IbvSge, IbvWcOpcode, LookUpBy, MrMetadata, SendRecv};
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

    receiver.connect()?;
    receiver.qp_list[0].state()?;
    info!("expecting metadata at address: {}, rkey: {}", receiver.metadata_addr(), receiver.metadata_rkey());

    let sge = IbvSge::new(receiver.metadata_addr(), MrMetadata::SIZE as u32, receiver.metadata_lkey());
    let notify_wr = IbvRecvWr::new(0,sge,1);
    receiver.qp_list[0].ibv_post_recv(notify_wr)?;
    receiver.qp_list[0].complete(1, IbvWcOpcode::RecvRdmaWithImm, SendRecv::Recv)?;
    info!("Event received");
    //info!("{:#?}",receiver.get_receiver_metadata());
    let ptr = receiver.metadata_addr() as *mut MrMetadata;
    let mr_metadata = unsafe{&*ptr};
    info!("{:#?}",mr_metadata);
    Ok(())

}