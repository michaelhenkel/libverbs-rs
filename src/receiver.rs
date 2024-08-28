use std::{io::{Read, Write}, net::{IpAddr, TcpListener}, sync::Arc, thread};
use crate::{Hints, IbvAccessFlags, IbvDevice, IbvMr, IbvPd, IbvQp, IbvSendWr, IbvWcOpcode, IbvWrOpcode, LookUpBy, MrMetadata, QpMetadata, QpMode, SendRecv, SocketComm, SocketCommCommand};

pub struct Receiver{
    device: IbvDevice,
    listen_socket_port: u16,
    pub receiver_metadata: MrMetadata,
    receiver_metadata_mr: Option<IbvMr>,
    receiver_metadata_address: u64,
    receiver_metadata_rkey: u32,
    pub sender_metadata_address: u64,
    pub sender_metadata_rkey: u32,
    pub pd: Arc<IbvPd>,
    pub qp_list: Vec<IbvQp>,
    qp_metadata_list: Vec<QpMetadata>,
    qp_mode: QpMode,
}

impl Receiver {
    pub fn new(look_up_by: LookUpBy, listen_socket_port: u16, qp_mode: QpMode) -> anyhow::Result<Receiver> {
        let device = IbvDevice::new(look_up_by)?;
        if device.context.as_ptr().is_null() {
            return Err(anyhow::anyhow!("Device context is null"));
        }
        let pd = Arc::new(IbvPd::new(device.context()));
        let receiver_metadata = MrMetadata{
            address: 7,
            rkey: 8,
            padding: 9,
            length: 10,
        };

        Ok(Receiver{
            device,
            listen_socket_port,
            receiver_metadata,
            receiver_metadata_address: 0,
            receiver_metadata_rkey: 0,
            sender_metadata_address: 0,
            sender_metadata_rkey: 0,
            receiver_metadata_mr: None,
            pd: pd.clone(),
            qp_list: Vec::new(),
            qp_metadata_list: Vec::new(),
            qp_mode,
        })
    }
    pub fn create_metadata_mr(&mut self) -> anyhow::Result<()> {
        let access_flags = IbvAccessFlags::LocalWrite.as_i32() | IbvAccessFlags::RemoteWrite.as_i32() | IbvAccessFlags::RemoteRead.as_i32();
        let receiver_metadata_mr = IbvMr::new(self.pd.clone(), &self.receiver_metadata, MrMetadata::SIZE, access_flags);
        let receiver_metadata_address = receiver_metadata_mr.addr();
        let receiver_metadata_rkey = receiver_metadata_mr.rkey();
        self.receiver_metadata_mr = Some(receiver_metadata_mr.clone());
        self.receiver_metadata_address = receiver_metadata_address;
        self.receiver_metadata_rkey = receiver_metadata_rkey;
        Ok(())
    }
    pub fn get_receiver_metadata_mr(&self) -> Option<IbvMr> {
        self.receiver_metadata_mr.clone()
    }
    pub fn get_receiver_metadata_address(&self) -> u64 {
        let ptr = &self.receiver_metadata as *const MrMetadata;
        ptr as u64
    }
    pub fn pd(&self) -> Arc<IbvPd> {
        Arc::clone(&self.pd)
    }
    pub fn get_receiver_metadata(&self) -> MrMetadata {
        self.receiver_metadata.clone()
    }
    pub fn listen(&mut self, hints: Hints) -> anyhow::Result<()> {
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
                    let recv_metadata = MrMetadata{
                        address: self.receiver_metadata_address,
                        rkey: self.receiver_metadata_rkey,
                        padding: 0,
                        length: 0,
                    };
                    in_tx.send(SocketCommCommand::Mr(recv_metadata)).unwrap();
                },
                SocketCommCommand::InitQp(idx, family) => {
                    let gid_idx = match self.qp_mode{
                        QpMode::Multi => {idx},
                        QpMode::Single => {0}
                    };
                    let gid_entry = self.device.gid_table.get_entry_by_index(gid_idx as usize, family.clone());
                    if let Some((_ip_addr, gid_entry)) = gid_entry{
                        let qp = IbvQp::new(self.pd(), self.device.context(), gid_entry.gidx(), gid_entry.port());
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
                        return Err(anyhow::anyhow!("No GID entry found for index {} and family {:?}", gid_idx, family));
                    }
                },
                SocketCommCommand::ConnectQp(qp_metadata) => {
                    self.qp_metadata_list.push(qp_metadata);
                    in_tx.send(SocketCommCommand::Continue).unwrap();
                },
                SocketCommCommand::Stop => { 
                    in_tx.send(SocketCommCommand::Stop).unwrap();
                    break; 
                },
                SocketCommCommand::Continue => { continue; },
            }
        }
        Ok(())
    }
    pub fn connect(&mut self) -> anyhow::Result<()> {
        for (qp_idx, qp) in self.qp_list.iter().enumerate() {
            let remote_qp_metadata = self.qp_metadata_list.get(qp_idx).unwrap();
            let new_metadata = MrMetadata::default();
            let mr = IbvMr::new(self.pd.clone(), &new_metadata, MrMetadata::SIZE, IbvAccessFlags::LocalWrite.as_i32() | IbvAccessFlags::RemoteWrite.as_i32() | IbvAccessFlags::RemoteRead.as_i32());
            qp.connect(remote_qp_metadata)?;
            let send_wr = IbvSendWr::new(
                &mr,
                self.sender_metadata_address,
                self.sender_metadata_rkey,
                IbvWrOpcode::Send,
            );
            qp.ibv_post_send(send_wr.as_ptr())?;
            qp.complete(1, IbvWcOpcode::Send, SendRecv::Send)?;
        }
        Ok(())
    }
    pub fn metadata_addr(&self) -> u64 {
        self.receiver_metadata_mr.as_ref().unwrap().addr()
    }
    pub fn metadata_lkey(&self) -> u32 {
        self.receiver_metadata_mr.as_ref().unwrap().lkey()
    }
    pub fn metadata_rkey(&self) -> u32 {
        self.receiver_metadata_mr.as_ref().unwrap().rkey()
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
                if let SocketCommCommand::Continue = local_socket_comm {
                    continue;
                }
                let serialized = bincode::serialize(&local_socket_comm).unwrap();
                stream.write_all(&serialized).unwrap();
            }
            break;
        }
    });
    Ok(())
}