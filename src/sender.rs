use std::{ffi::c_void, io::{self, Read, Write}, net::{IpAddr, SocketAddr, TcpStream}, os::fd::RawFd, sync::{atomic::AtomicU32, Arc}};
use rdma_sys::{ibv_async_event, ibv_async_event_element_t, ibv_event_type, ibv_get_async_event};

use crate::{ControlBuffer, ControlBufferMetadata, ControlBufferTrait, Family, IbvAccessFlags, IbvCompChannel, IbvCq, IbvDevice, IbvEventType, IbvMr, IbvPd, IbvQp, InBuffer, LookUpBy, OutBuffer, QpMetadata, QpMode, SocketComm, SocketCommCommand};

pub struct Sender{
    id: u32,
    pub connection_id: u32,
    control_buffer: ControlBuffer,
    number_of_requests: u64,
    pub device: Box<IbvDevice>,
    receiver_socket_address: IpAddr,
    receiver_socket_port: u16,
    mrs: u32,
    pub pd: Arc<IbvPd>,
    pub qp_list: Vec<IbvQp>,
    pub num_qps: u32,
    family: Family,
    qp_mode: QpMode,
    rate_limit: Option<u32>,
    qp_health_tracker: Arc<AtomicU32>,
    shared_cq: Option<Arc<IbvCq>>,
    tcp_stream: Option<TcpStream>,
    pub chassis_id: Option<[u8; 6]>,
    pub remote_chassis_id: [u8; 6],
}

impl Sender {
    pub fn new<C: ControlBufferTrait>(look_up_by: LookUpBy, receiver_socket_address: IpAddr, receiver_socket_port: u16, num_qps: u32, family: Family, qp_mode: QpMode, rate_limit: Option<u32>, shared_cq: bool) -> anyhow::Result<Sender> {
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
        let cq = if shared_cq{
            let comp_channel = Arc::new(IbvCompChannel::new(&device.context()));
            let cq = Arc::new(IbvCq::new(&device.context, 4096, &comp_channel, 0));
            Some(cq)
        } else {
            None
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
            rate_limit,
            qp_health_tracker: Arc::new(AtomicU32::new(0)),
            shared_cq: cq,
            tcp_stream: None,
            chassis_id: None,
            remote_chassis_id: [0; 6],
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
    pub fn close_tcp_stream(&mut self){
        self.tcp_stream = None;
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
    pub fn receive_remote_qp_metadata_and_connect(&mut self) -> anyhow::Result<bool>{
        let mut buffer = vec![0; 1024];
        match self.tcp_stream.as_mut().unwrap().read(&mut buffer) {
            Ok(_) => {},
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                return Ok(false);
            }
            Err(e) => return Err(e.into()),
        }
        let socket_comm: SocketComm = bincode::deserialize(&buffer).unwrap();
        if let SocketCommCommand::InitQp(_remote_num_qps, remote_qp_metadata_list ) = socket_comm.command {
            for (qp_idx, qp) in self.qp_list.iter_mut().enumerate(){
                let remote_qp_metadata = &remote_qp_metadata_list[qp_idx];
                qp.connect(remote_qp_metadata)?;
            }
        }
        Ok(true)
    }
    pub fn init_qps_and_send_qp_metadata(&mut self) -> anyhow::Result<bool> {
        let local_chassis_id = match self.chassis_id{
            Some(chassis_id) => chassis_id,
            None => [0; 6],
        };
        let num_qps = if local_chassis_id == self.remote_chassis_id && local_chassis_id != [0; 6] && self.remote_chassis_id != [0; 6]{
            1
        } else {
            self.num_qps
        };
        let mut qp_metadata_list = Vec::new();
        for qp_idx in 0..num_qps{
            let gid_idx = match self.qp_mode {
                QpMode::Single => 0,
                QpMode::Multi => qp_idx,
            };
            let gid_entry = self.device.gid_table.get_entry_by_index(gid_idx as usize, self.family.clone());
            if let Some((ip_addr, gid_entry)) = gid_entry{
                let mut qp = IbvQp::new(self.pd(), self.device.context(), gid_entry.gidx(), gid_entry.port(), self.rate_limit, self.shared_cq.clone());
                qp.local_gid = ip_addr.to_string();
                qp.hca_name = self.device.name.clone();
                qp.init(gid_entry.port)?;
                let subnet_id = gid_entry.subnet_id();
                let interface_id = gid_entry.interface_id();
                let qpn = qp.qp_num();
                let psn = qp.psn();
                let qp_metadata = QpMetadata{
                    subnet_id,
                    interface_id,
                    qpn,
                    psn,
                    family: self.family.clone(),
                    qp_mode: self.qp_mode.clone(),
                };
                self.qp_list.push(qp);
                qp_metadata_list.push(qp_metadata);
            }
        }
        let socket_comm = SocketComm{
            command: crate::SocketCommCommand::InitQp(num_qps, qp_metadata_list),
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
    pub fn receive_control_buffer_and_remote_chassis_id(&mut self) -> anyhow::Result<bool> {
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
            self.connection_id = self.connection_id;
            self.remote_chassis_id = control_buffer_metadata.chassis_id;
        }
        Ok(true)
    }
    pub fn send_control_buffer(&mut self) -> anyhow::Result<bool>{
        let control_buffer_metadata = ControlBufferMetadata{
            in_address: self.control_buffer.in_buffer.local_addr,
            in_rkey: self.control_buffer.in_buffer.local_rkey,
            out_address: self.control_buffer.out_buffer.local_addr,
            out_rkey: self.control_buffer.out_buffer.local_rkey,
            length: self.control_buffer.in_buffer.length as u64,
            nreq: 0,
            receiver_id: 0,
            chassis_id: self.chassis_id.unwrap_or_default(),
        };
        let socket_comm = SocketComm{
            command: crate::SocketCommCommand::Mr(control_buffer_metadata),
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
    pub fn connect(&mut self) -> anyhow::Result<bool> {
        if self.chassis_id.is_none(){
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
            self.chassis_id = Some(chassis_id);
        }
        if self.tcp_stream.is_none(){
            let socket_address = SocketAddr::new(self.receiver_socket_address, self.receiver_socket_port);
            let tcp_stream = match TcpStream::connect(socket_address) {
                Ok(s) => {
                    s.set_nonblocking(true)?;
                    s
                }
                Err(e) => {
                    match e.kind() {
                        io::ErrorKind::WouldBlock => {
                            return Ok(false);
                        }
                        io::ErrorKind::ConnectionRefused => { return Ok(false); }
                        _ => {
                            println!("Error connecting to receiver: {}", e);
                            return Err(e.into());
                        }
                    }
                },
            };
            self.tcp_stream = Some(tcp_stream);
        }
        Ok(true)
    }
    pub fn event_tracker(&self) -> anyhow::Result<()> {
        let context = self.device.context.clone();
        let async_fd: RawFd = unsafe { (*(*context.inner)).async_fd };
        let qp_list = self.qp_list.clone();
        let qp_health_tracker = self.qp_health_tracker.clone();
        std::thread::spawn(move || {
            use libc::{poll, pollfd, POLLIN};
            let mut fds = [pollfd {
                fd: async_fd,
                events: POLLIN,
                revents: 0,
            }];
            loop {
                // Wait for an event on async_fd
                let ret = unsafe { poll(fds.as_mut_ptr(), 1, -1) };
                if ret < 0 {
                    // Handle error
                    eprintln!("Error polling async_fd");
                    break;
                }
                if fds[0].revents & POLLIN != 0 {
                    // An event is available
                    let element = unsafe { std::mem::zeroed::<ibv_async_event_element_t>()};
                    let event_type = unsafe { std::mem::zeroed::<ibv_event_type>()};
                    let mut event = ibv_async_event {
                        event_type,
                        element,
                    };
                    let ret = unsafe { ibv_get_async_event(context.as_ptr(), &mut event) };
                    if ret != 0 {
                        // Handle error
                        eprintln!("Error getting async event");
                        break;
                    }
                    match event.event_type {
                        ibv_event_type::IBV_EVENT_QP_FATAL |
                        ibv_event_type::IBV_EVENT_QP_REQ_ERR |
                        ibv_event_type::IBV_EVENT_QP_ACCESS_ERR => {
                            // Check if the event is related to your QP
                            for (qp_idx, qp) in qp_list.iter().enumerate() {
                                if unsafe { event.element.qp == qp.as_ptr() }{
                                    println!("sender QP {} event {:?} state: {:?}", qp_idx, IbvEventType::from(event.event_type), qp.state().unwrap());
                                    qp_health_tracker.fetch_or(1 << qp_idx, std::sync::atomic::Ordering::SeqCst);
                                }
                            }
                        },
                        ibv_event_type::IBV_EVENT_PORT_ERR => {
                            qp_health_tracker.fetch_or((1 << qp_list.len()) - 1, std::sync::atomic::Ordering::SeqCst);
                            for (qp_idx, qp) in qp_list.iter().enumerate() {
                                println!("sender QP {} state: {:?}", qp_idx, qp.state().unwrap());
                            }
                            println!("sender received port error event");
                        },
                        ibv_event_type::IBV_EVENT_GID_CHANGE => {
                            for (qp_idx, qp) in qp_list.iter().enumerate() {
                                println!("sender QP {} state: {:?}", qp_idx, qp.state().unwrap());
                            }
                            println!("sender received GID change event");
                        },
                        _ => {
                            let et = IbvEventType::from(event.event_type);
                            // Handle other events if necessary
                            println!("sender received good event {:?}", et);
                        }
                    }
                }
            }
        });
        Ok(())
    }
    pub fn qp_health_tracker(&self) -> Arc<AtomicU32> {
        self.qp_health_tracker.clone()
    }
    pub fn device_healthy(&self) -> bool {
        self.qp_health_tracker.load(std::sync::atomic::Ordering::SeqCst) == 0
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