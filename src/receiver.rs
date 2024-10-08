use std::{cell::RefCell, ffi::c_void, io::{Read, Write}, net::{IpAddr, Ipv4Addr, TcpListener}, os::fd::RawFd, result, sync::{atomic::AtomicU32, mpsc, Arc, Mutex, RwLock}, thread::{self, JoinHandle}};
use rdma_sys::{ibv_async_event, ibv_async_event_element_t, ibv_event_type, ibv_get_async_event};
use crossbeam::channel;

use crate::{ControlBuffer, ControlBufferMetadata, ControlBufferTrait, Hints, IbvAccessFlags, IbvCompChannel, IbvCq, IbvDevice, IbvEventType, IbvMr, IbvPd, IbvQp, InBuffer, LookUpBy, OutBuffer, QpMetadata, QpMode, SocketComm, SocketCommCommand};

pub trait ReceiverInterface {
    fn listen_address(&self) -> String;
    fn qp_health_tracker(&self) -> Arc<AtomicU32>;
    fn event_tracker(&self) -> anyhow::Result<()>;
    fn device_healthy(&self) -> bool;
    fn accept(&mut self) -> anyhow::Result<()>;
    fn in_buffer_ptr(&self) -> *mut c_void;
    fn out_buffer_ptr(&self) -> *mut c_void;
    fn in_buffer_mr(&self) -> IbvMr;
    fn out_buffer_mr(&self) -> IbvMr;
    fn connection_id(&self) -> u32;
    fn pd(&self) -> Arc<IbvPd>;
    fn num_qps(&self) -> usize;
    fn qp_list(&self) -> Vec<IbvQp>;
    fn get_qp(&self, idx: usize) -> IbvQp;
    fn in_remote_buffer_addr(&self) -> u64;
    fn in_remote_buffer_rkey(&self) -> u32;
}

impl ReceiverInterface for Receiver {
    fn listen_address(&self) -> String {
        self.listen_address()
    }
    fn qp_health_tracker(&self) -> Arc<AtomicU32> {
        self.qp_health_tracker()
    }
    fn event_tracker(&self) -> anyhow::Result<()> {
        self.event_tracker()
    }
    fn device_healthy(&self) -> bool {
        self.device_healthy()
    }
    fn accept(&mut self) -> anyhow::Result<()> {
        self.accept()
    }
    fn num_qps(&self) -> usize {
        self.qp_list.len()
    }
    fn qp_list(&self) -> Vec<IbvQp> {
        self.qp_list.clone()
    }
    fn in_buffer_ptr(&self) -> *mut c_void {
        self.in_buffer_ptr()
    }
    fn out_buffer_ptr(&self) -> *mut c_void {
        self.out_buffer_ptr()
    }
    fn in_buffer_mr(&self) -> IbvMr {
        self.in_buffer_mr()
    }
    fn out_buffer_mr(&self) -> IbvMr {
        self.out_buffer_mr()
    }
    fn connection_id(&self) -> u32 {
        self.connection_id()
    }
    fn pd(&self) -> Arc<IbvPd> {
        self.pd()
    }
    fn get_qp(&self, idx: usize) -> IbvQp {
        self.qp_list.get(idx).unwrap().clone()
    }
    fn in_remote_buffer_addr(&self) -> u64 {
        self.in_remote_buffer_addr()
    }
    fn in_remote_buffer_rkey(&self) -> u32 {
        self.in_remote_buffer_rkey()
    }
}
pub struct Receiver{
    id: u32,
    pub connection_id: u32,
    control_buffer: ControlBuffer,
    number_of_requests: u64,
    device: IbvDevice,
    listen_socket_port: u16,
    listen_address: IpAddr,
    mrs: u32,
    pub pd: Arc<IbvPd>,
    pub qp_list: Vec<IbvQp>,
    join_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    qp_metadata_list: Vec<QpMetadata>,
    qp_mode: QpMode,
    rate_limit: Option<u32>,
    qp_health_tracker: Arc<AtomicU32>,
    shared_cq: Option<Arc<IbvCq>>,
    pub result_recv: Arc<RwLock<channel::Receiver<(Vec<IbvQp>, Vec<QpMetadata>, u64, u32, u64, u32)>>>,
    pub result_send: channel::Sender<(Vec<IbvQp>, Vec<QpMetadata>, u64, u32, u64, u32)>,
}

impl Receiver {
    pub fn new<C: ControlBufferTrait>(look_up_by: LookUpBy, listen_socket_port: u16, qp_mode: QpMode, rate_limit: Option<u32>, shared_cq: bool) -> anyhow::Result<Receiver> {
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
            join_handle: Arc::new(Mutex::new(None)),
            qp_metadata_list: Vec::new(),
            qp_mode,
            rate_limit,
            qp_health_tracker: Arc::new(AtomicU32::new(0)),
            shared_cq: cq,
            result_recv: Arc::new(RwLock::new(result_recv)),
            result_send,
        };
        Ok(receiver)
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
        let (out_tx, out_rx) = std::sync::mpsc::channel();
        let (in_tx, in_rx) = std::sync::mpsc::channel();
        socket_listener(address, self.listen_socket_port, out_tx, in_rx).map_err(|e| anyhow::anyhow!("Error creating listener thread: {:?}", e))?;
        let device = self.device.clone();
        let in_address = self.control_buffer.in_buffer.local_addr;
        let in_rkey = self.control_buffer.in_buffer.local_rkey;
        let out_address = self.control_buffer.out_buffer.local_addr;
        let out_rkey = self.control_buffer.out_buffer.local_rkey;
        let connection_id = self.connection_id;
        let pd = self.pd();
        let qp_mode = self.qp_mode.clone();
        let receiver_id = self.get_id();
        let shared_cq = self.shared_cq.clone();
        let rate_limit = self.rate_limit.clone();
        let result_send = self.result_send.clone();
        let jh: JoinHandle<()> = thread::spawn(move || {
            let mut qp_list = Vec::new();
            let mut in_remote_address = 0;
            let mut in_remote_rkey = 0;
            let mut out_remote_address = 0;
            let mut out_remote_rkey = 0;
            let mut qp_metadata_list: Vec<QpMetadata> = Vec::new();
            while let Ok(socket_command) = out_rx.recv() {
                let pd = pd.clone();
                match socket_command{
                    SocketCommCommand::Mr(control_buffer_metadata) => {
                        in_remote_address = control_buffer_metadata.in_address;
                        in_remote_rkey = control_buffer_metadata.in_rkey;
                        out_remote_address = control_buffer_metadata.out_address;
                        out_remote_rkey = control_buffer_metadata.out_rkey;
                        let recv_metadata = ControlBufferMetadata{
                            in_address,
                            in_rkey,
                            out_address,
                            out_rkey,
                            length: 0,
                            nreq: 0,
                            receiver_id,
                            connection_id,
                        };
                        in_tx.send(SocketCommCommand::Mr(recv_metadata)).unwrap();
                    },
                    SocketCommCommand::InitQp(idx, family) => {
                        let gid_idx = match qp_mode{
                            QpMode::Multi => {idx},
                            QpMode::Single => {0}
                        };
                        let gid_entry = device.gid_table.get_entry_by_index(gid_idx as usize, family.clone());
                        if let Some((ip_addr, gid_entry)) = gid_entry{
                            let mut qp = IbvQp::new(pd, device.context(), gid_entry.gidx(), gid_entry.port(), rate_limit, shared_cq.clone());
                            qp.local_gid = ip_addr.to_string();
                            qp.hca_name = device.name.clone();
                            qp.init(gid_entry.port).unwrap();
                            let qpn = qp.qp_num();
                            let psn = qp.psn();
                            qp_list.push(qp);
                            let subnet_id = gid_entry.subnet_id();
                            let interface_id = gid_entry.interface_id();
                            let qp_metadata = QpMetadata{
                                subnet_id,
                                interface_id,
                                qpn,
                                psn
                            };
                            in_tx.send(SocketCommCommand::ConnectQp(qp_metadata)).unwrap();
                        }
                    },
                    SocketCommCommand::ConnectQp(qp_metadata) => {
                        qp_metadata_list.push(qp_metadata);
                        in_tx.send(SocketCommCommand::Continue).unwrap();
                    },
                    SocketCommCommand::Stop => {
                        in_tx.send(SocketCommCommand::Stop).unwrap();
                        break; 
                    },
                    SocketCommCommand::Continue => { continue; },
                }
            }
            let _ = result_send.send((qp_list, qp_metadata_list, in_remote_address, in_remote_rkey, out_remote_address, out_remote_rkey));
            //(qp_list, qp_metadata_list, in_remote_address, in_remote_rkey, out_remote_address, out_remote_rkey)
        });
        self.join_handle = Arc::new(Mutex::new(Some(jh)));
        Ok(())
    }
    pub fn accept(&mut self) -> anyhow::Result<()> {
        let result_recv = self.result_recv.clone();
        let result_recv = result_recv.read().unwrap();
        let (qp_list, qp_metadata_list, in_remote_address, in_remote_key, out_remote_address, out_remote_key) = match result_recv.recv_timeout(std::time::Duration::from_micros(50)){
            Ok(result) => {
                result
            },
            Err(e) => {
                return Err(anyhow::anyhow!("Error receiving result: {:?}", e));
            }
        };
        self.qp_list = qp_list;
        self.control_buffer.in_buffer.remote_addr = in_remote_address;
        self.control_buffer.in_buffer.remote_rkey = in_remote_key;
        self.control_buffer.out_buffer.remote_addr = out_remote_address;
        self.control_buffer.out_buffer.remote_rkey = out_remote_key;
        self.qp_metadata_list = qp_metadata_list;
        for (qp_idx, qp) in self.qp_list.iter_mut().enumerate() {
            let remote_qp_metadata = self.qp_metadata_list.get(qp_idx).unwrap();
            qp.connect(remote_qp_metadata)?;
        }
        Ok(())
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
                                    println!("receiver QP {} event {:?} state: {:?}", qp_idx, IbvEventType::from(event.event_type), qp.state().unwrap());
                                    qp_health_tracker.fetch_or(1 << qp_idx, std::sync::atomic::Ordering::SeqCst);
                                }
                            }
                        },
                        ibv_event_type::IBV_EVENT_PORT_ERR => {
                            qp_health_tracker.fetch_or((1 << qp_list.len()) - 1, std::sync::atomic::Ordering::SeqCst);
                            for (qp_idx, qp) in qp_list.iter().enumerate() {
                                println!("receiver QP {} state: {:?}", qp_idx, qp.state().unwrap());
                            }
                            println!("receiver received port error event");
                        },
                        ibv_event_type::IBV_EVENT_GID_CHANGE => {
                            for (qp_idx, qp) in qp_list.iter().enumerate() {
                                println!("receiver QP {} state: {:?}", qp_idx, qp.state().unwrap());
                            }
                            println!("receiver received GID change event");
                        },
                        _ => {
                            let et = IbvEventType::from(event.event_type);
                            // Handle other events if necessary
                            println!("receiver received good event {:?}", et);
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