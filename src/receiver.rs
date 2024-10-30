use std::{ffi::c_void, io::{self, Read, Write}, net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream}, sync::{atomic::AtomicU32, Arc, RwLock}};
use crossbeam::channel;
use crate::{ConnectionStages, ControlBuffer, ControlBufferMetadata, ControlBufferTrait, Hints, IbvAccessFlags, IbvCompChannel, IbvCq, IbvDevice, IbvMr, IbvPd, IbvQp, InBuffer, LookUpBy, OutBuffer, QpMetadata, QpMode, SocketComm, SocketCommCommand};

pub struct Receiver{
    id: u32,
    pub connection_id: u32,
    pub control_buffer: ControlBuffer,
    number_of_requests: u64,
    pub device: IbvDevice,
    listen_socket_port: u16,
    listen_address: IpAddr,
    mrs: u32,
    pub pd: Arc<IbvPd>,
    pub qp_list: Vec<IbvQp>,
    qp_metadata_list: Vec<QpMetadata>,
    remote_qp_metadata_list: Vec<QpMetadata>,
    rate_limit: Option<u32>,
    qp_health_tracker: Arc<AtomicU32>,
    shared_cq: Option<Arc<IbvCq>>,
    pub init: bool,
    pub result_recv: Arc<RwLock<channel::Receiver<(Vec<IbvQp>, Vec<QpMetadata>, u64, u32, u64, u32)>>>,
    pub result_send: channel::Sender<(Vec<IbvQp>, Vec<QpMetadata>, u64, u32, u64, u32)>,
    pub listener: Option<TcpListener>,
    pub tcp_stream: Option<TcpStream>,
    pub stage: ConnectionStages,
}

impl Receiver {
    pub fn new<C: ControlBufferTrait>(look_up_by: LookUpBy, listen_socket_port: u16, rate_limit: Option<u32>, shared_cq: bool) -> anyhow::Result<Receiver> {
        let device = IbvDevice::new(look_up_by)?;
        if device.context.as_ptr().is_null() {
            return Err(anyhow::anyhow!("Device context is null"));
        }
        let pd = Arc::new(IbvPd::new(device.context()));
        let buffer_len = C::size() as u64;
        let in_buffer = InBuffer{
            local_addr: 0,
            local_rkey: 0,
            local_lkey: 0,
            length: buffer_len,
            buffer: C::new(),
            remote_addr: 0,
            remote_rkey: 0,
            mr: None,
        };
        let out_buffer = OutBuffer{
            local_addr: 0,
            local_rkey: 0,
            local_lkey: 0,
            length: buffer_len,
            buffer: C::new(),
            remote_addr: 0,
            remote_rkey: 0,
            mr: None,
        };
        let control_buffer = ControlBuffer{
            in_buffer,
            out_buffer,
        };
        let cq = if shared_cq{
            let comp_channel = Arc::new(IbvCompChannel::new(&device.context()));
            let cq = Arc::new(IbvCq::new(&device.context, 4096, &comp_channel, 0));
            Some(cq)
        } else {
            None
        };
        let (result_send, result_recv) = channel::bounded(10);
        let receiver = Receiver{
            id: rand::random::<u32>(),
            connection_id: rand::random::<u32>(),
            control_buffer,
            number_of_requests: 0,
            device,
            listen_socket_port,
            listen_address: IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
            mrs: 0,
            pd,
            qp_list: Vec::new(),
            remote_qp_metadata_list: Vec::new(),
            qp_metadata_list: Vec::new(),
            rate_limit,
            qp_health_tracker: Arc::new(AtomicU32::new(0)),
            shared_cq: cq,
            result_recv: Arc::new(RwLock::new(result_recv)),
            result_send,
            listener: None,
            tcp_stream: None,
            stage: ConnectionStages::Init,
            init: false,
        };
        Ok(receiver)
    }
    pub fn close_listener_and_tcp_stream(&mut self){
        if let Some(listener) = self.listener.take(){
            drop(listener);
        }
        if let Some(tcp_stream) = self.tcp_stream.take(){
            drop(tcp_stream);
        }
    }
    pub fn reset_nreqs(&mut self) {
        self.number_of_requests = 0;
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
        let ptr: *mut () = &*self.control_buffer.out_buffer.buffer as *const dyn ControlBufferTrait as *mut ();
        ptr as *mut c_void
    }
    pub fn out_local_buffer_addr(&self) -> u64 {
        self.control_buffer.out_buffer.local_addr
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
    pub fn listen_address(&self) -> String {
        format!("{:?}:{}", self.listen_address, self.listen_socket_port)
    }
    pub fn create_control_buffer(&mut self) -> anyhow::Result<()> {
        let access_flags = IbvAccessFlags::LocalWrite.as_i32() | IbvAccessFlags::RemoteWrite.as_i32() | IbvAccessFlags::RemoteRead.as_i32();
        let in_buffer_addr = self.in_buffer_ptr();
        let in_buffer_length = self.control_buffer.in_buffer.length;
        let in_buffer_mr = IbvMr::new(self.pd.clone(), in_buffer_addr, in_buffer_length as usize, access_flags);
        self.control_buffer.in_buffer.local_addr = in_buffer_mr.addr();
        self.control_buffer.in_buffer.local_rkey = in_buffer_mr.rkey();
        self.control_buffer.in_buffer.local_lkey = in_buffer_mr.lkey();
        self.control_buffer.in_buffer.mr = Some(in_buffer_mr);


        let out_buffer_addr = self.out_buffer_ptr();
        let out_buffer_length = self.control_buffer.out_buffer.length;
        let out_buffer_mr = IbvMr::new(self.pd.clone(), out_buffer_addr, out_buffer_length as usize, access_flags);
        self.control_buffer.out_buffer.local_addr = out_buffer_mr.addr();
        self.control_buffer.out_buffer.local_rkey = out_buffer_mr.rkey();
        self.control_buffer.out_buffer.local_lkey = out_buffer_mr.lkey();
        self.control_buffer.out_buffer.mr = Some(out_buffer_mr);
        Ok(())
    }
    pub fn pd(&self) -> Arc<IbvPd> {
        Arc::clone(&self.pd)
    }
    pub fn get_listen_address(&self) -> IpAddr {
        self.listen_address.clone()
    }
    pub fn listen(&mut self, hints: Hints) -> anyhow::Result<()> {
        if self.listener.is_some() {
            return Ok(());
        }
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
        self.listen_address = address;
        let socket_address = SocketAddr::new(address, self.listen_socket_port);
        let listener = TcpListener::bind(socket_address)?;
        listener.set_nonblocking(true)?;
        self.listener = Some(listener);
        Ok(())
    }

    pub fn send_control_buffer_and_chassis_id(&mut self) -> anyhow::Result<bool> {
        let chassis_id = lldpd_rs::get_remote_chassis_id(&self.device.kernel_name);
        let chassis_id = match chassis_id{
            Some(chassis_id) => {
                let parts: Vec<&str> = chassis_id.split(':').collect();
                if parts.len() == 6 {
                    let mut chassis_id = [0; 6];
                    for (idx, part) in parts.iter().enumerate() {
                        chassis_id[idx] = u8::from_str_radix(part, 16).unwrap();
                    }
                    chassis_id
                } else {
                    [0; 6]
                }
            },
            None => {
                [0; 6]
            },
        };
        let socket_comm = SocketComm{
            command: SocketCommCommand::Mr(ControlBufferMetadata{
                in_address: self.control_buffer.in_buffer.local_addr,
                in_rkey: self.control_buffer.in_buffer.local_rkey,
                out_address: self.control_buffer.out_buffer.local_addr,
                out_rkey: self.control_buffer.out_buffer.local_rkey,
                length: self.control_buffer.in_buffer.length as u64,
                nreq: 0,
                receiver_id: 0,
                chassis_id,
            }),
        };
        let serialized = bincode::serialize(&socket_comm).unwrap();
        match self.tcp_stream.as_mut().unwrap().write(&serialized) {
            Ok(_) => {},
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                return Ok(false);
            }
            Err(e) => return Err(e.into()),
        }
        Ok(true)
    }
    pub fn receive_control_buffer(&mut self) -> anyhow::Result<bool>{
        let mut buffer = vec![0; 1024];
        match self.tcp_stream.as_mut().unwrap().read(&mut buffer) {
            Ok(_) => {},
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                return Ok(false);
            }
            Err(e) => return Err(e.into()),
        }
        let socket_comm: SocketComm = bincode::deserialize(&buffer).unwrap();
        if let SocketCommCommand::Mr(control_buffer_metadata) = socket_comm.command {
            self.control_buffer.in_buffer.remote_addr = control_buffer_metadata.in_address;
            self.control_buffer.in_buffer.remote_rkey = control_buffer_metadata.in_rkey;
            self.control_buffer.out_buffer.remote_addr = control_buffer_metadata.out_address;
            self.control_buffer.out_buffer.remote_rkey = control_buffer_metadata.out_rkey;
        }
        Ok(true)
    }
    pub fn receive_qp_metadata_and_init_qps(&mut self) -> anyhow::Result<bool>{
        let mut buffer = vec![0; 1024];
        match self.tcp_stream.as_mut().unwrap().read(&mut buffer) {
            Ok(_) => {},
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                return Ok(false);
            }
            Err(e) => return Err(e.into()),
        }
        let socket_comm: SocketComm = bincode::deserialize(&buffer).unwrap();
        if let SocketCommCommand::InitQp(_num_qps, qp_metadata_list ) = socket_comm.command {
            for (qp_idx, remote_qp_metadata) in qp_metadata_list.iter().enumerate() {
                self.remote_qp_metadata_list.push(remote_qp_metadata.clone());
                let gid_idx = match remote_qp_metadata.qp_mode{
                    QpMode::Multi => {qp_idx},
                    QpMode::Single => {0}
                };
                let pd = self.pd.clone();
                let gid_entry = self.device.gid_table.get_entry_by_index(gid_idx as usize, remote_qp_metadata.family.clone());
                if let Some((ip_addr, gid_entry)) = gid_entry{
                    let mut qp = IbvQp::new(pd, self.device.context(), gid_entry.gidx(), gid_entry.port(), self.rate_limit, self.shared_cq.clone());
                    qp.local_gid = ip_addr.to_string();
                    qp.hca_name = self.device.name.clone();
                    qp.init(gid_entry.port).unwrap();
                    let subnet_id = gid_entry.subnet_id();
                    let interface_id = gid_entry.interface_id();
                    let qpn = qp.qp_num();
                    let psn = qp.psn();
                    let qp_metadata = QpMetadata{
                        subnet_id,
                        interface_id,
                        qpn,
                        psn,
                        family: remote_qp_metadata.family.clone(),
                        qp_mode: remote_qp_metadata.qp_mode.clone(),
                    };
                    self.qp_metadata_list.push(qp_metadata);
                    self.qp_list.push(qp);
                }
            }
        }
        Ok(true)
    }
    pub fn send_qp_metadata_and_connect_qps(&mut self) -> anyhow::Result<bool> {
        let num_qps = self.qp_list.len() as u32;
        let socket_comm = SocketComm{
            command: crate::SocketCommCommand::InitQp(num_qps, self.qp_metadata_list.clone()),
        };
        let serialized = bincode::serialize(&socket_comm).unwrap();
        match self.tcp_stream.as_mut().unwrap().write(&serialized) {
            Ok(_) => {},
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                return Ok(false);
            }
            Err(e) => return Err(e.into()),
        }
        for (qp_idx, qp) in self.qp_list.iter_mut().enumerate() {
            qp.connect(self.remote_qp_metadata_list.get(qp_idx).unwrap())?;
        }
        Ok(true)
    }
    pub fn accept(&mut self) -> anyhow::Result<bool> {
        if self.tcp_stream.is_some() {
            return Ok(true);
        }
        match self.listener.as_mut().unwrap().accept(){
            Ok((tcp_stream, _)) => {
                tcp_stream.set_nonblocking(true)?;
                self.tcp_stream = Some(tcp_stream);
            },
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                return Ok(false);
            }
            Err(e) => return Err(e.into()),
        }
        Ok(true)
    }
    pub fn qp_health_tracker(&self) -> Arc<AtomicU32> {
        self.qp_health_tracker.clone()
    }
    pub fn device_healthy(&self) -> bool {
        self.qp_health_tracker.load(std::sync::atomic::Ordering::SeqCst) == 0
    }

}