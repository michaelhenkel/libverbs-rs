use std::{ffi::c_void, io::{Read, Write}, net::{IpAddr, Ipv4Addr, TcpListener}, pin::{self, Pin}, sync::{Arc, Mutex}, thread::{self, JoinHandle}};
use crate::{receiver, ControlBuffer, ControlBufferMetadata, ControlBufferTrait, Hints, IbvAccessFlags, IbvDevice, IbvMr, IbvPd, IbvQp, IbvSendWr, IbvWcOpcode, IbvWrOpcode, InBuffer, LookUpBy, OutBuffer, QpMetadata, QpMode, SendRecv, SocketComm, SocketCommCommand, SLOT_COUNT};

pub trait ReceiverInterface {
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
    connection_id: u32,
    control_buffer: ControlBuffer,
    number_of_requests: u64,
    device: IbvDevice,
    listen_socket_port: u16,
    listen_address: IpAddr,
    mrs: u32,
    pub pd: Arc<IbvPd>,
    pub qp_list: Vec<IbvQp>,
    join_handle: Arc<Mutex<Option<JoinHandle<(Vec<IbvQp>, Vec<QpMetadata>, u64, u32, u64, u32)>>>>,
    qp_metadata_list: Vec<QpMetadata>,
    qp_mode: QpMode,
}

impl Receiver {
    pub fn new<C: ControlBufferTrait>(look_up_by: LookUpBy, listen_socket_port: u16, qp_mode: QpMode) -> anyhow::Result<Receiver> {
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
        println!("Listening on address: {:?}:{}", address, self.listen_socket_port);

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
        let jh: JoinHandle<(Vec<IbvQp>, Vec<QpMetadata>, u64, u32, u64, u32)> = thread::spawn(move || {
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
                        if let Some((_ip_addr, gid_entry)) = gid_entry{
                            let qp = IbvQp::new(pd, device.context(), gid_entry.gidx(), gid_entry.port());
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
            (qp_list, qp_metadata_list, in_remote_address, in_remote_rkey, out_remote_address, out_remote_rkey)
        });
        self.join_handle = Arc::new(Mutex::new(Some(jh)));
        Ok(())
    }
    pub fn accept(&mut self) -> anyhow::Result<()> {
        let jh = self.join_handle.lock().unwrap().take();
        let (qp_list, qp_metadata_list, in_remote_address, in_remote_key, out_remote_address, out_remote_key) = jh.unwrap().join().unwrap();
        self.qp_list = qp_list;
        self.control_buffer.in_buffer.remote_addr = in_remote_address;
        self.control_buffer.in_buffer.remote_rkey = in_remote_key;
        self.control_buffer.out_buffer.remote_addr = out_remote_address;
        self.control_buffer.out_buffer.remote_rkey = out_remote_key;
        self.qp_metadata_list = qp_metadata_list;
        for (qp_idx, qp) in self.qp_list.iter().enumerate() {
            let remote_qp_metadata = self.qp_metadata_list.get(qp_idx).unwrap();
            qp.connect(remote_qp_metadata)?;
        }
        Ok(())
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