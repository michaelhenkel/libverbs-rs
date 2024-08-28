use std::{io::{Read, Write}, net::{IpAddr, TcpStream}, sync::Arc};
use crate::{Family, IbvAccessFlags, IbvDevice, IbvMr, IbvPd, IbvQp, IbvRecvWr, IbvSge, IbvWcOpcode, LookUpBy, MrMetadata, QpMetadata, SendRecv, SocketComm, SocketCommCommand};

pub struct Sender{
    device: IbvDevice,
    receiver_socket_address: IpAddr,
    receiver_socket_port: u16,
    sender_metadata: MrMetadata,
    sender_metadata_mr: Option<IbvMr>,
    pub receiver_metadata_address: u64,
    pub receiver_metadata_rkey: u32,
    pub pd: Arc<IbvPd>,
    pub qp_list: Vec<IbvQp>,
    num_qps: u32,
    family: Family
}

impl Sender {
    pub fn new(look_up_by: LookUpBy, receiver_socket_address: IpAddr, receiver_socket_port: u16, num_qps: u32, family: Family) -> anyhow::Result<Sender> {
        let device = IbvDevice::new(look_up_by)?;
        let pd = Arc::new(IbvPd::new(device.context()));
        let sender_metadata = MrMetadata::default();
        let access_flags = IbvAccessFlags::LocalWrite.as_i32() | IbvAccessFlags::RemoteWrite.as_i32() | IbvAccessFlags::RemoteRead.as_i32();
        let sender_metadata_mr = IbvMr::new(pd.clone(), &sender_metadata, MrMetadata::SIZE, access_flags);
        Ok(Sender{
            device,
            receiver_socket_address,
            receiver_socket_port,
            sender_metadata,
            receiver_metadata_address: 0,
            receiver_metadata_rkey: 0,
            sender_metadata_mr: None,
            pd,
            qp_list: Vec::new(),
            num_qps,
            family
        })
    }
    pub fn create_metadata(&mut self) -> anyhow::Result<()> {
        let access_flags = IbvAccessFlags::LocalWrite.as_i32() | IbvAccessFlags::RemoteWrite.as_i32() | IbvAccessFlags::RemoteRead.as_i32();
        let sender_metadata_mr = IbvMr::new(self.pd.clone(), &self.sender_metadata, MrMetadata::SIZE, access_flags);
        self.sender_metadata_mr = Some(sender_metadata_mr.clone());
        self.sender_metadata.address = sender_metadata_mr.addr();
        self.sender_metadata.rkey = sender_metadata_mr.rkey();
        Ok(())
    }
    pub fn get_sender_metadata(&self) -> MrMetadata {
        self.sender_metadata.clone()
    }
    pub fn set_metadata_address(&mut self, addr: u64) {
        self.sender_metadata.address = addr;
    }
    pub fn set_metadata_rkey(&mut self, rkey: u32) {
        self.sender_metadata.rkey = rkey;
    }
    pub fn set_metadata_length(&mut self, length: u64) {
        self.sender_metadata.length = length;
    }
    pub fn metadata_addr(&self) -> u64 {
        self.sender_metadata_mr.as_ref().unwrap().addr()
    }
    pub fn metadata_lkey(&self) -> u32 {
        self.sender_metadata_mr.as_ref().unwrap().lkey()
    }
    pub fn pd(&self) -> Arc<IbvPd> {
        Arc::clone(&self.pd)
    }
    pub fn connect(&mut self) -> anyhow::Result<()> {
        
        let send_address = if self.receiver_socket_address.is_ipv4() {
            format!("{}:{}", self.receiver_socket_address, self.receiver_socket_port)
        } else {
            format!("[{}]:{}", self.receiver_socket_address, self.receiver_socket_port)
        };
        let mut stream = TcpStream::connect(send_address).unwrap();
        let meta_data = MrMetadata{
            address: self.sender_metadata_mr.as_ref().unwrap().addr(),
            rkey: self.sender_metadata_mr.as_ref().unwrap().rkey(),
            padding: 0,
            length: 0,
        };
        let socket_comm = SocketComm{
            command: crate::SocketCommCommand::Mr(meta_data),
        };
        let serialized = bincode::serialize(&socket_comm).unwrap();
        stream.write(&serialized).unwrap();
        let mut buffer = vec![0; 1024];
        stream.read(&mut buffer).unwrap();
        let socket_comm: SocketComm = bincode::deserialize(&buffer).unwrap();
        if let SocketCommCommand::Mr(metadata) = socket_comm.command {
            self.receiver_metadata_address = metadata.address;
            self.receiver_metadata_rkey = metadata.rkey;
        }
        for qp_idx in 0..self.num_qps {
            let gid_entry = self.device.gid_table.get_entry_by_index(qp_idx as usize, self.family.clone());
            if let Some((_ip_addr, gid_entry)) = gid_entry{
                let qp = IbvQp::new(self.pd(), self.device.context(), gid_entry.gidx(), gid_entry.port());
                qp.init(gid_entry.port)?;
                let socket_comm = SocketComm{
                    command: crate::SocketCommCommand::InitQp(qp_idx, self.family.clone()),
                };
                let serialized = bincode::serialize(&socket_comm).unwrap();
                stream.write(&serialized).unwrap();
                let mut buffer = vec![0; 1024];
                stream.read(&mut buffer).unwrap();
                let socket_comm: SocketComm = bincode::deserialize(&buffer).unwrap();
                if let SocketCommCommand::ConnectQp(remote_qp_metadata) = socket_comm.command {
                    qp.connect(&remote_qp_metadata)?;
                    let subnet_id = gid_entry.subnet_id();
                    let interface_id = gid_entry.interface_id();
                    let qpn = qp.qp_num();
                    let psn = qp.psn();
                    let qp_metadata = QpMetadata{
                        subnet_id,
                        interface_id,
                        qpn,
                        psn
                    };
                    self.qp_list.push(qp);
                    let sock_comm = SocketComm{
                        command: crate::SocketCommCommand::ConnectQp(qp_metadata),
                    };
                    let serialized = bincode::serialize(&sock_comm).unwrap();
                    stream.write(&serialized).unwrap();
                    
                }
            }
        }
        let socket_comm = SocketComm{
            command: crate::SocketCommCommand::Stop,
        };
        let serialized = bincode::serialize(&socket_comm).unwrap();
        stream.write(&serialized).unwrap();
        for qp in &self.qp_list{
            let sge = IbvSge::new(self.metadata_addr(), MrMetadata::SIZE as u32, self.metadata_lkey());
            let notify_wr = IbvRecvWr::new(0,sge,1);
            qp.ibv_post_recv(notify_wr)?;
            qp.complete(1, IbvWcOpcode::Recv, SendRecv::Recv)?;
        }
        Ok(())
    }
    pub fn destroy(&mut self) -> anyhow::Result<()> {
        for qp in &self.qp_list {
            qp.event_channel().destroy();
            qp.recv_cq().destroy();
            qp.destroy();
        }
        self.pd.destroy();

        self.device.destroy();

        Ok(())
    }
}