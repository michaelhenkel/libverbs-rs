use std::{io::{Read, Write}, net::{IpAddr, TcpListener}, thread};
use log::info;

use crate::{Hints, IbvAccessFlags, IbvDevice, IbvMr, IbvPd, IbvQp, IbvSendWr, IbvSge, IbvWrOpcode, LookUpBy, MrMetadata, QpMetadata, SocketComm, SocketCommCommand};

pub struct Receiver{
    device: IbvDevice,
    listen_socket_port: u16,
    receiver_metadata: MrMetadata,
    receiver_metadata_mr: IbvMr,
    sender_metadata_address: u64,
    sender_metadata_rkey: u32,
    pub pd: IbvPd,
    pub qp_list: Vec<IbvQp>,
    qp_metadata_list: Vec<QpMetadata>,
}

impl Receiver {
    pub fn new(look_up_by: LookUpBy, listen_socket_port: u16) -> anyhow::Result<Receiver> {
        let device = IbvDevice::new(look_up_by)?;
        info!("Receiver created device");
        // check is device context is not null
        if device.context.as_ptr().is_null() {
            return Err(anyhow::anyhow!("Device context is null"));
        }
        let pd = IbvPd::new(&device.context);
        info!("Receiver created pd");
        let receiver_metadata = MrMetadata::default();
        info!("Receiver created metadata");
        let access_flags = IbvAccessFlags::LocalWrite.as_i32() | IbvAccessFlags::RemoteWrite.as_i32() | IbvAccessFlags::RemoteRead.as_i32();
        let receiver_metadata_mr = IbvMr::new(&pd, receiver_metadata.addr(), MrMetadata::SIZE, access_flags);
        info!("Receiver created metadata memory region with addr: {}, rkey: {}", receiver_metadata_mr.addr(), receiver_metadata_mr.rkey());
        Ok(Receiver{
            device,
            listen_socket_port,
            receiver_metadata,
            sender_metadata_address: 0,
            sender_metadata_rkey: 0,
            receiver_metadata_mr,
            pd,
            qp_list: Vec::new(),
            qp_metadata_list: Vec::new(),
        })
    }
    pub fn listen(&mut self, hints: Hints) -> anyhow::Result<()> {
        info!("Receiver starts to listen on port {}", self.listen_socket_port);
        let address = match hints{
            Hints::Address(address) => {
                address
            },
            Hints::AddressFamily(family) => {
                let gid_entry = self.device.gid_table.get_entry_by_family(family.clone());
                match gid_entry {
                    Some((address, _gid_entry)) => {
                        address
                    },
                    None => {
                        return Err(anyhow::anyhow!("No GID entry found for family {:?}", family));
                    }
                }
            },
        };

        let (out_tx, out_rx) = std::sync::mpsc::channel();
        let (in_tx, in_rx) = std::sync::mpsc::channel();
        socket_listener(address, self.listen_socket_port, out_tx, in_rx).map_err(|e| anyhow::anyhow!("Error creating listener thread: {:?}", e))?;
        while let Ok(socket_command) = out_rx.recv() {
            match socket_command{
                SocketCommCommand::Mr(metadata) => {
                    self.sender_metadata_address = metadata.address;
                    self.sender_metadata_rkey = metadata.rkey;
                    info!("Receiver received metadata from sender: address: {}, rkey: {}", self.sender_metadata_address, self.sender_metadata_rkey);
                    let recv_metadata = MrMetadata{
                        address: self.receiver_metadata_mr.addr(),
                        rkey: self.receiver_metadata_mr.rkey(),
                        length: 0,
                    };
                    info!("Receiver sending metadata to sender: address: {}, rkey: {}", recv_metadata.address, recv_metadata.rkey);
                    in_tx.send(SocketCommCommand::Mr(recv_metadata)).unwrap();
                },
                SocketCommCommand::InitQp(idx, family) => {
                    info!("Receiver received qp init command");
                    let gid_entry = self.device.gid_table.get_entry_by_index(idx as usize, family.clone());
                    if let Some((_ip_addr, gid_entry)) = gid_entry{
                        let qp = IbvQp::new(&self.pd, &self.device.context, gid_entry.gidx(), gid_entry.port());
                        qp.init(gid_entry.port)?;
                        let qpn = qp.qp_num();
                        let psn = qp.psn();
                        self.qp_list.push(qp);
                        let subnet_id = gid_entry.subnet_id();
                        let interface_id = gid_entry.interface_id();
                        let qp_metadata = QpMetadata{
                            subnet_id,
                            interface_id,
                            qpn,
                            psn
                        };
                        in_tx.send(SocketCommCommand::ConnectQp(qp_metadata)).unwrap();
                    } else {
                        return Err(anyhow::anyhow!("No GID entry found for index {} and family {:?}", idx, family));
                    }
                },
                SocketCommCommand::ConnectQp(qp_metadata) => {
                    info!("Receiver received qp connect command with remote qp metadata: {:?}", qp_metadata);
                    self.qp_metadata_list.push(qp_metadata);
                    in_tx.send(SocketCommCommand::ConnectQp(QpMetadata::default())).unwrap();
                },
                SocketCommCommand::Stop => { 
                    in_tx.send(SocketCommCommand::Stop).unwrap();
                    info!("Receiver received stop command");
                    break; 
                },
            }
        }
        Ok(())
    }
    pub fn connect(&mut self) -> anyhow::Result<()> {
        for (qp_idx, qp) in self.qp_list.iter().enumerate() {
            let remote_qp_metadata = self.qp_metadata_list.get(qp_idx).unwrap();
            qp.connect(remote_qp_metadata)?;
            let sge = IbvSge::new(self.metadata_addr(), MrMetadata::SIZE as u32, self.metadata_lkey());
            let flags = IbvAccessFlags::LocalWrite.as_i32() | IbvAccessFlags::RemoteWrite.as_i32() | IbvAccessFlags::RemoteRead.as_i32();
            let send_wr = IbvSendWr::new(
                0,
                sge,
                1,
                IbvWrOpcode::Send,
                flags,
                self.sender_metadata_address,
                self.sender_metadata_rkey,
            );
            info!("Receiver posting send");
            qp.ibv_post_send(send_wr)?;
            info!("Receiver send posted");
        }
        Ok(())
    }
    pub fn metadata_addr(&self) -> u64 {
        self.receiver_metadata_mr.addr()
    }
    pub fn metadata_lkey(&self) -> u32 {
        self.receiver_metadata_mr.lkey()
    }
    pub fn metadata_rkey(&self) -> u32 {
        self.receiver_metadata_mr.rkey()
    }
}

fn socket_listener(listen_address: IpAddr, port: u16, out_tx: std::sync::mpsc::Sender<SocketCommCommand>, in_rx: std::sync::mpsc::Receiver<SocketCommCommand>) -> anyhow::Result<()>{
    let address = format!("{}:{}", listen_address, port);
    thread::spawn(move || {
        let listener = TcpListener::bind(address).unwrap();
        for stream in listener.incoming() {
            let mut stream = stream.unwrap();
            loop{
                let mut buffer = vec![0; 1024]; // Adjust size if necessary
                let bytes_read = stream.read(&mut buffer).unwrap();
                if bytes_read == 0 {
                    break;
                }
                let remote_socket_comm: SocketComm = bincode::deserialize(&buffer).unwrap();
                out_tx.send(remote_socket_comm.command).unwrap();
                let local_socket_comm = in_rx.recv().unwrap();
                if let SocketCommCommand::Stop = local_socket_comm {
                    break;
                }
                let serialized = bincode::serialize(&local_socket_comm).unwrap();
                stream.write_all(&serialized).unwrap();
            }
            break;
        }
    });
    Ok(())
}