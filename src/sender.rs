use std::{ffi::c_void, io::{Read, Write}, net::{IpAddr, TcpStream}, sync::Arc};
use crate::{ControlBuffer, ControlBufferMetadata, ControlBufferTrait, Family, IbvAccessFlags, IbvDevice, IbvMr, IbvPd, IbvQp, InBuffer, LookUpBy, OutBuffer, QpMetadata, QpMode, SocketComm, SocketCommCommand};


pub trait SenderInterface{
    fn pd(&self) -> Arc<IbvPd>;
    fn in_buffer_ptr(&self) -> *mut c_void;
    fn out_buffer_ptr(&self) -> *mut c_void;
    fn in_buffer_mr(&self) -> IbvMr;
    fn out_buffer_mr(&self) -> IbvMr;
    fn in_remote_buffer_addr(&self) -> u64;
    fn in_remote_buffer_rkey(&self) -> u32;
    fn connection_id(&self) -> u32;
    fn get_qp(&self, qp_idx: usize) -> IbvQp;
    fn num_qps(&self) -> u32;
    fn qps(&self) -> Vec<IbvQp>;
}

impl SenderInterface for Sender{
    fn pd(&self) -> Arc<IbvPd> {
        Arc::clone(&self.pd)
    }
    fn in_buffer_ptr(&self) -> *mut c_void {
        let ptr: *mut () = &*self.control_buffer.in_buffer.buffer as *const dyn ControlBufferTrait as *mut ();
        ptr as *mut c_void
    }
    fn out_buffer_ptr(&self) -> *mut c_void {
        let ptr = &*self.control_buffer.out_buffer.buffer as *const dyn ControlBufferTrait as *mut ();
        ptr as *mut c_void
    }
    fn in_remote_buffer_addr(&self) -> u64 {
        self.control_buffer.in_buffer.remote_addr
    }
    fn in_remote_buffer_rkey(&self) -> u32 {
        self.control_buffer.in_buffer.remote_rkey
    }
    fn in_buffer_mr(&self) -> IbvMr {
        self.control_buffer.in_buffer.mr.as_ref().unwrap().clone()
    }
    fn out_buffer_mr(&self) -> IbvMr {
        self.control_buffer.out_buffer.mr.as_ref().unwrap().clone()
    }
    fn connection_id(&self) -> u32 {
        self.connection_id
    }
    fn get_qp(&self, qp_idx: usize) -> IbvQp {
        self.qp_list[qp_idx].clone()
    }
    fn num_qps(&self) -> u32 {
        self.num_qps
    }
    fn qps(&self) -> Vec<IbvQp> {
        self.qp_list.clone()
    }
}
pub struct Sender{
    id: u32,
    connection_id: u32,
    control_buffer: ControlBuffer,
    number_of_requests: u64,
    device: Box<IbvDevice>,
    receiver_socket_address: IpAddr,
    receiver_socket_port: u16,
    mrs: u32,
    pub pd: Arc<IbvPd>,
    pub qp_list: Vec<IbvQp>,
    num_qps: u32,
    family: Family,
    qp_mode: QpMode,
}

impl Sender {
    pub fn new<C: ControlBufferTrait>(look_up_by: LookUpBy, receiver_socket_address: IpAddr, receiver_socket_port: u16, num_qps: u32, family: Family, qp_mode: QpMode) -> anyhow::Result<Sender> {
        let device = Box::new(IbvDevice::new(look_up_by)?);
        let pd = Arc::new(IbvPd::new(device.context()));
        
        let buffer_len = C::size() as u64;
        let control_buffer = ControlBuffer{
            in_buffer: InBuffer{
                local_addr: 0,
                local_rkey: 0,
                local_lkey: 0,
                length: buffer_len,
                buffer: C::new(),
                remote_addr: 0,
                remote_rkey: 0,
                mr: None,
            },
            out_buffer: OutBuffer{
                local_addr: 0,
                local_rkey: 0,
                local_lkey: 0,
                length: buffer_len,
                buffer: C::new(),
                remote_addr: 0,
                remote_rkey: 0,
                mr: None,
            },
        };

        Ok(Sender{
            id: rand::random::<u32>(),
            control_buffer,
            connection_id: 0,
            number_of_requests: 0,
            device,
            receiver_socket_address,
            receiver_socket_port,
            mrs: 0,
            pd,
            qp_list: Vec::new(),
            num_qps,
            family,
            qp_mode,
        })
    }
    pub fn num_qps(&self) -> u32 {
        self.num_qps
    }
    pub fn qps(&self) -> Vec<IbvQp> {
        self.qp_list.clone()
    }
    pub fn mrs(&self) -> u32 {
        self.mrs
    }
    pub fn incr_mrs(&mut self) {
        self.mrs += 1;
    }
    pub fn connection_id(&self) -> u32 {
        self.connection_id
    }
    pub fn get_id(&self) -> u32 {
        self.id
    }
    pub fn nreqs(&self) -> u64 {
        self.number_of_requests
    }
    pub fn inc_nreqs(&mut self) {
        self.number_of_requests += 1;
    }
    pub fn dec_nreqs(&mut self) {
        self.number_of_requests -= 1;
    }
    pub fn create_control_buffer(&mut self) -> anyhow::Result<()> {
        let access_flags = IbvAccessFlags::LocalWrite.as_i32() | IbvAccessFlags::RemoteWrite.as_i32() | IbvAccessFlags::RemoteRead.as_i32();
        let in_buffer_addr = self.in_buffer_ptr();
        let in_buffer_mr = IbvMr::new(self.pd.clone(), in_buffer_addr, self.control_buffer.in_buffer.length as usize, access_flags);
        self.control_buffer.in_buffer.local_addr = in_buffer_mr.addr();
        self.control_buffer.in_buffer.local_rkey = in_buffer_mr.rkey();
        self.control_buffer.in_buffer.local_lkey = in_buffer_mr.lkey();
        self.control_buffer.in_buffer.mr = Some(in_buffer_mr);

        let out_buffer_addr = self.out_buffer_ptr();
        let out_buffer_mr = IbvMr::new(self.pd.clone(), out_buffer_addr, self.control_buffer.out_buffer.length as usize, access_flags);
        self.control_buffer.out_buffer.local_addr = out_buffer_mr.addr();
        self.control_buffer.out_buffer.local_rkey = out_buffer_mr.rkey();
        self.control_buffer.out_buffer.local_lkey = out_buffer_mr.lkey();
        self.control_buffer.out_buffer.mr = Some(out_buffer_mr);
        Ok(())
    }
    pub fn reset_nreqs(&mut self) {
        self.number_of_requests = 0;
    }
    pub fn pd(&self) -> Arc<IbvPd> {
        Arc::clone(&self.pd)
    }
    pub fn out_buffer_mr(&self) -> IbvMr {
        self.control_buffer.out_buffer.mr.as_ref().unwrap().clone()
    }
    pub fn out_remote_buffer_addr(&self) -> u64 {
        self.control_buffer.out_buffer.remote_addr
    }
    pub fn out_remote_buffer_rkey(&self) -> u32 {
        self.control_buffer.out_buffer.remote_rkey
    }
    pub fn out_buffer_ptr(&self) -> *mut c_void {
        let ptr = &*self.control_buffer.out_buffer.buffer as *const dyn ControlBufferTrait as *mut ();
        ptr as *mut c_void
    }
    pub fn in_buffer_mr(&self) -> IbvMr {
        self.control_buffer.in_buffer.mr.as_ref().unwrap().clone()
    }
    pub fn in_remote_buffer_addr(&self) -> u64 {
        self.control_buffer.in_buffer.remote_addr
    }
    pub fn in_remote_buffer_rkey(&self) -> u32 {
        self.control_buffer.in_buffer.remote_rkey
    }
    pub fn in_local_buffer_addr(&self) -> u64 {
        self.control_buffer.in_buffer.local_addr
    }
    pub fn in_buffer_ptr(&self) -> *mut c_void {
        let ptr: *mut () = &*self.control_buffer.in_buffer.buffer as *const dyn ControlBufferTrait as *mut ();
        ptr as *mut c_void
    }
    pub fn connect(&mut self) -> anyhow::Result<()> {
        let send_address = if self.receiver_socket_address.is_ipv4() {
            format!("{}:{}", self.receiver_socket_address, self.receiver_socket_port)
        } else {
            format!("[{}]:{}", self.receiver_socket_address, self.receiver_socket_port)
        };
        let mut stream = match TcpStream::connect(send_address.clone()){
            Ok(stream) => stream,
            Err(e) => {
                println!("Failed to connect to receiver: {:?} {}", e, send_address);
                return Err(anyhow::anyhow!("Failed to connect to receiver: {:?}", e));
            }
        };
        let control_buffer_metadata = ControlBufferMetadata{
            in_address: self.control_buffer.in_buffer.local_addr,
            in_rkey: self.control_buffer.in_buffer.local_rkey,
            out_address: self.control_buffer.out_buffer.local_addr,
            out_rkey: self.control_buffer.out_buffer.local_rkey,
            length: self.control_buffer.in_buffer.length as u64,
            nreq: 0,
            receiver_id: 0,
            connection_id: 0,
        };
        let socket_comm = SocketComm{
            command: crate::SocketCommCommand::Mr(control_buffer_metadata),
        };
        let serialized = bincode::serialize(&socket_comm).unwrap();
        stream.write(&serialized).unwrap();
        let mut buffer = vec![0; 1024];
        stream.read(&mut buffer).unwrap();
        
        let socket_comm: SocketComm = bincode::deserialize(&buffer).unwrap();
        if let SocketCommCommand::Mr(control_buffer_metadata) = socket_comm.command {
            self.control_buffer.in_buffer.remote_addr = control_buffer_metadata.in_address;
            self.control_buffer.in_buffer.remote_rkey = control_buffer_metadata.in_rkey;
            self.control_buffer.out_buffer.remote_addr = control_buffer_metadata.out_address;
            self.control_buffer.out_buffer.remote_rkey = control_buffer_metadata.out_rkey;
            self.connection_id = control_buffer_metadata.connection_id;
        }
        for qp_idx in 0..self.num_qps {
            let gid_idx = match self.qp_mode {
                QpMode::Single => 0,
                QpMode::Multi => qp_idx,
            };
            let gid_entry = self.device.gid_table.get_entry_by_index(gid_idx as usize, self.family.clone());
            if let Some((_ip_addr, gid_entry)) = gid_entry{
                let mut qp = IbvQp::new(self.pd(), self.device.context(), gid_entry.gidx(), gid_entry.port());
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
        
        /*/
        for qp in &self.qp_list{
            let notify_wr = IbvRecvWr::new(&self.out_buffer_mr());
            qp.ibv_post_recv(notify_wr)?;
        }*/     
        Ok(())
    }
    /* 
    pub fn destroy(&mut self) -> anyhow::Result<()> {
        for qp in &self.qp_list {
            qp.event_channel().destroy();
            qp.recv_cq().destroy();
            qp.send_cq().destroy();
            qp.destroy();
        }
        self.pd.destroy();
        self.device.destroy();
        Ok(())
    }
    */
}