use std::{collections::BTreeMap, ffi::CStr, fs, net::{IpAddr, Ipv4Addr, Ipv6Addr}, ops::BitOr, path::PathBuf, ptr::{self, null_mut, NonNull}};
use log::info;
use rdma_sys::*;
use serde::{Deserialize, Serialize};
use std::sync::Once;
use env_logger::Env;
//use log::LevelFilter;

static INIT: Once = Once::new();

pub fn initialize_logger() {
    INIT.call_once(|| {
        env_logger::Builder::from_env(Env::default().default_filter_or("info"))
            //.filter_module("my_library", LevelFilter::Info)
            .init();
    });
}

pub mod sender;
pub mod receiver;

pub struct IbvQp{
    inner: Box<*mut ibv_qp>,
    recv_cq: IbvCq,
    send_cq: IbvCq,
    event_channel: IbvCompChannel,
    psn: u32,
    gidx: i32,
    port: u8,
}

impl IbvQp{
    pub fn new(pd: &IbvPd, context: &IbvContext, gidx: i32, port: u8) -> Self{
        let comp_channel = IbvCompChannel::new(&context);
        info!("comp_channel created");
        let cq = IbvCq::new(&context, 100, &comp_channel, 0);
        let ret = unsafe { ibv_req_notify_cq(cq.as_ptr(), 0) };
        if ret != 0 {
            panic!("Failed to request notify cq");
        }
        info!("cq created");
        let mut qp_init_attr = ibv_qp_init_attr {
            qp_context: null_mut(),
            send_cq: cq.as_ptr(),
            recv_cq: cq.as_ptr(),
            srq: null_mut(),
            cap: ibv_qp_cap {
                max_send_wr: 100,
                max_recv_wr: 100,
                max_send_sge: 15,
                max_recv_sge: 15,
                max_inline_data: 64,
            },
            qp_type: ibv_qp_type::IBV_QPT_RC,
            sq_sig_all: 0,
        };
        info!("qp_init_attr created");
        let qp = unsafe{ ibv_create_qp(pd.as_ptr(), &mut qp_init_attr) };
        info!("qp created: {}", unsafe {
            (*qp).qp_num
        });
        let inner = Box::new(qp);
        let psn = rand::random::<u32>() & 0xffffff;
        IbvQp{
            inner,
            recv_cq: cq.clone(),
            send_cq: cq,
            event_channel: comp_channel,
            psn,
            gidx,
            port,
        }
    }
    pub fn as_ptr(&self) -> *mut ibv_qp {
        *self.inner
    }
    pub fn qp_num(&self) -> u32{
        unsafe{ (*self.as_ptr()).qp_num }
    }
    pub fn psn(&self) -> u32{
        self.psn
    }
    pub fn init(&self, port: u8) -> anyhow::Result<()>{
        let mut qp_attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
        qp_attr.qp_state = ibv_qp_state::IBV_QPS_INIT;
        qp_attr.pkey_index = 0;
        qp_attr.port_num = port;
        qp_attr.qp_access_flags = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0 | ibv_access_flags::IBV_ACCESS_REMOTE_READ.0 | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE.0;
        let qp_attr_mask = ibv_qp_attr_mask::IBV_QP_STATE | ibv_qp_attr_mask::IBV_QP_PKEY_INDEX | ibv_qp_attr_mask::IBV_QP_PORT | ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;
        let ret = unsafe { ibv_modify_qp(self.as_ptr(), &mut qp_attr, qp_attr_mask.0 as i32) };
        if ret != 0 {
            return Err(anyhow::anyhow!("Failed to modify QP"));
        }
        Ok(())
    }
    pub fn connect(&self, remote_qp_metadata: &QpMetadata) -> anyhow::Result<()>{
        let remote_subnet_id = remote_qp_metadata.subnet_id;
        let remote_interface_id = remote_qp_metadata.interface_id;
        let subnet_prefix_bytes = remote_subnet_id.to_be_bytes();
        let interface_id_bytes = remote_interface_id.to_be_bytes();
        let subnet_prefix_bytes = subnet_prefix_bytes.iter().rev().cloned().collect::<Vec<u8>>();
        let interface_id_bytes = interface_id_bytes.iter().rev().cloned().collect::<Vec<u8>>();
        let mut raw = [0u8; 16];
        raw[..8].copy_from_slice(&subnet_prefix_bytes);
        raw[8..].copy_from_slice(&interface_id_bytes);
        let remote_gid = ibv_gid{
            raw,
        };
        let remote_qpn = remote_qp_metadata.qpn;
        let remote_psn = remote_qp_metadata.psn;
        let psn = self.psn;
        let gidx = self.gidx;
        let port = self.port;
        let mut qp_attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
        qp_attr.qp_state = ibv_qp_state::IBV_QPS_RTR;
        qp_attr.path_mtu = ibv_mtu::IBV_MTU_4096;
        qp_attr.dest_qp_num = remote_qpn;
        qp_attr.rq_psn = remote_psn;
        qp_attr.max_dest_rd_atomic = 1;
        qp_attr.min_rnr_timer = 12;
        qp_attr.ah_attr.sl = 0;
        qp_attr.ah_attr.src_path_bits = 0;
        qp_attr.ah_attr.port_num = port;
        qp_attr.ah_attr.dlid = 0;
        qp_attr.ah_attr.grh.dgid = remote_gid;
        qp_attr.ah_attr.is_global = 1;
        qp_attr.ah_attr.grh.sgid_index = gidx as u8;
        qp_attr.ah_attr.grh.hop_limit = 10;
        let qp_attr_mask = 
            ibv_qp_attr_mask::IBV_QP_STATE |
            ibv_qp_attr_mask::IBV_QP_AV |
            ibv_qp_attr_mask::IBV_QP_PATH_MTU |
            ibv_qp_attr_mask::IBV_QP_DEST_QPN |
            ibv_qp_attr_mask::IBV_QP_RQ_PSN |
            ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC |
            ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER;
        let ret = unsafe { ibv_modify_qp(self.as_ptr(), &mut qp_attr, qp_attr_mask.0 as i32) };
        if ret != 0 {
            return Err(anyhow::anyhow!("Failed to modify QP to RTR"));
        }
        qp_attr.qp_state = ibv_qp_state::IBV_QPS_RTS;
        qp_attr.timeout = 14;
        qp_attr.retry_cnt = 7;
        qp_attr.rnr_retry = 7;
        qp_attr.sq_psn = psn;
        qp_attr.max_rd_atomic = 1;
        let qp_attr_mask = 
            ibv_qp_attr_mask::IBV_QP_STATE |
            ibv_qp_attr_mask::IBV_QP_TIMEOUT |
            ibv_qp_attr_mask::IBV_QP_RETRY_CNT |
            ibv_qp_attr_mask::IBV_QP_RNR_RETRY |
            ibv_qp_attr_mask::IBV_QP_SQ_PSN |
            ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC;
        let ret = unsafe { ibv_modify_qp(self.as_ptr(), &mut qp_attr, qp_attr_mask.0 as i32) };
        if ret != 0 {
            return Err(anyhow::anyhow!("Failed to modify QP to RTS"));
        }
        Ok(())
    }
    pub fn ibv_post_send(&self, send_wr: IbvSendWr) -> anyhow::Result<()>{
        let mut bad_wr: *mut ibv_send_wr = ptr::null_mut();
        let ret = unsafe{ ibv_post_send(self.as_ptr(), send_wr.as_ptr(), &mut bad_wr) };
        if ret != 0 {
            return Err(anyhow::anyhow!("Failed to post send"));
        }
        Ok(())
    }
    pub fn ibv_post_recv(&self, recv_wr: IbvRecvWr) -> anyhow::Result<()>{
        let mut bad_wr: *mut ibv_recv_wr = ptr::null_mut();
        let ret = unsafe { ibv_post_recv(self.as_ptr(), recv_wr.as_ptr(), &mut bad_wr) };
        if ret != 0 {
            println!("Error posting recv: {}", ret);
        }
        Ok(())
    }
    pub fn wait_for_event(&self) -> anyhow::Result<()>{
        let event_channel = self.event_channel.as_ptr();
        let mut recv_cq = self.recv_cq.as_ptr();
        let mut cq = unsafe { std::mem::zeroed::<*mut ibv_cq>() };
        info!("Waiting for cq event");
        
        let mut context = null_mut();
        let ret = unsafe{ ibv_get_cq_event(event_channel, &mut cq, &mut context) };
        if ret != 0 {
            return Err(anyhow::anyhow!("Failed to get cq event"));
        }
        
        info!("Requesting notify cq");
        let ret = unsafe{ ibv_req_notify_cq(recv_cq, 0) };
        if ret != 0 {
            return Err(anyhow::anyhow!("Failed to request notify cq"));
        }
        let mut wc = unsafe { std::mem::zeroed::<ibv_wc>() };
        info!("Polling cq");
        let ret = unsafe{ ibv_poll_cq(recv_cq, 1, &mut wc) };
        if ret != 1 {
            return Err(anyhow::anyhow!("Failed to poll cq"));
        }
        if wc.status != ibv_wc_status::IBV_WC_SUCCESS {
            return Err(anyhow::anyhow!("Failed to get success status"));
        }
        Ok(())
    }
    pub fn state(&self) -> anyhow::Result<()>{
        let state = unsafe{ (*self.as_ptr()).state };
        info!("QP state: {:?}", state);
        Ok(())
    }
}

impl Drop for IbvQp{
    fn drop(&mut self){
        info!("Destroying QP");
        unsafe{ ibv_destroy_qp(self.as_ptr()) };
    }
}

unsafe impl Send for IbvQp{}
unsafe impl Sync for IbvQp{}

pub struct IbvQpInitAttr{
    inner: ibv_qp_init_attr,
}

impl IbvQpInitAttr{
    pub fn new(
        send_cp: &IbvCq,
        recv_cq: &IbvCq,
        max_send_wr: u32,
        max_recv_wr: u32,
        max_send_sge: u32,
        max_recv_sge: u32,
        max_inline_data: u32,
        qp_type: IbvQpType,
        sq_sig_all: i32,
    ) -> Self{
        IbvQpInitAttr{
            inner: ibv_qp_init_attr{
                qp_context: ptr::null_mut(),
                send_cq: send_cp.as_ptr(),
                recv_cq: recv_cq.as_ptr(),
                srq: ptr::null_mut(),
                cap: ibv_qp_cap{
                    max_send_wr,
                    max_recv_wr,
                    max_send_sge,
                    max_recv_sge,
                    max_inline_data,
                },
                qp_type: qp_type.get(),
                sq_sig_all,
            },
        }
    }

    pub fn as_ptr(&self) -> *mut ibv_qp_init_attr{
        &self.inner as *const _ as *mut _
    }
    pub fn as_ptr_mut(&mut self) -> &mut ibv_qp_init_attr{
        &mut self.inner
    }
}

unsafe impl Send for IbvQpInitAttr{}
unsafe impl Sync for IbvQpInitAttr{}

pub enum IbvQpType{
    Rc,
    Uc,
    Ud,
    RawPacket,
    XrcSend,
    XrcRecv,
    Driver,
}

impl IbvQpType{
    pub fn get(&self) -> ibv_qp_type::Type{
        match self{
            IbvQpType::Rc => ibv_qp_type::IBV_QPT_RC,
            IbvQpType::Uc => ibv_qp_type::IBV_QPT_UC,
            IbvQpType::Ud => ibv_qp_type::IBV_QPT_UD,
            IbvQpType::RawPacket => ibv_qp_type::IBV_QPT_RAW_PACKET,
            IbvQpType::XrcSend => ibv_qp_type::IBV_QPT_XRC_SEND,
            IbvQpType::XrcRecv => ibv_qp_type::IBV_QPT_XRC_RECV,
            IbvQpType::Driver => ibv_qp_type::IBV_QPT_DRIVER,
        }
    }
}

#[derive(Clone)]
pub struct IbvCq{
    inner: Box<*mut ibv_cq>,
}

impl IbvCq{
    pub fn new(context: &IbvContext, cqe: i32, channel: &IbvCompChannel, comp_vector: i32) -> Self{
        let cq = unsafe{ ibv_create_cq(context.as_ptr(), cqe, null_mut(), channel.as_ptr(), comp_vector) };
        let inner = Box::new(cq);
        IbvCq{
            inner,
        }
    }
    pub fn as_ptr(&self) -> *mut ibv_cq {
        *self.inner
    }
    pub fn as_ptr_mut(&self) -> &mut ibv_cq{
        unsafe{ &mut *self.as_ptr() }
    }
}

impl Drop for IbvCq{
    fn drop(&mut self){
        unsafe{ ibv_destroy_cq(self.as_ptr()) };
    }
}

unsafe impl Send for IbvCq{}
unsafe impl Sync for IbvCq{}

pub struct IbvCompChannel{
    inner: Box<*mut ibv_comp_channel>,
}

impl IbvCompChannel{
    pub fn new(context: &IbvContext) -> Self{
        let channel = unsafe{ ibv_create_comp_channel(context.as_ptr()) };
        let inner = Box::new(channel);
        IbvCompChannel{
            inner,
        }
    }
    pub fn as_ptr(&self) -> *mut ibv_comp_channel {
        *self.inner
    }
    pub fn as_ptr_mut(&self) -> &mut ibv_comp_channel{
        unsafe{ &mut *self.as_ptr() }
    }
}

impl Drop for IbvCompChannel{
    fn drop(&mut self){
        info!("Destroying comp channel");
        unsafe{ ibv_destroy_comp_channel(self.as_ptr()) };
    }
}

unsafe impl Send for IbvCompChannel{}
unsafe impl Sync for IbvCompChannel{}

pub struct IbvPd{
    inner: Box<*mut ibv_pd>,
}

impl IbvPd{
    pub fn new(context: &IbvContext) -> Self{
        let pd = unsafe{ ibv_alloc_pd(context.as_ptr()) };
        let inner = Box::new(pd);
        IbvPd{
            inner,
        }
    }
    pub fn as_ptr(&self) -> *mut ibv_pd {
        *self.inner
    }
}

impl Drop for IbvPd{
    fn drop(&mut self){
        info!("Destroying PD");
        unsafe{ ibv_dealloc_pd(self.as_ptr()) };
    }
}

unsafe impl Send for IbvPd{}
unsafe impl Sync for IbvPd{}

pub struct IbvGid{
    inner: ibv_gid,
}

impl IbvGid{
    pub fn new(gid: ibv_gid) -> Self{
        IbvGid{
            inner: gid,
        }
    }
}

unsafe impl Send for IbvGid{}
unsafe impl Sync for IbvGid{}

pub struct IbvDevice{
    inner: Box<*mut ibv_device>,
    gid_table: GidTable,
    context: IbvContext,
}

impl IbvDevice{
    pub fn new(look_up_by: LookUpBy) -> anyhow::Result<Self>{
        let (device, context, gid_table) = device_lookup(look_up_by)?;
        let context = IbvContext::from_context(context);
        let inner = Box::new(device);
        Ok(IbvDevice{
            inner,
            gid_table,
            context,
        })
    }
    pub fn as_ptr(&self) -> *mut ibv_device {
        *self.inner
    }
    pub fn gid_table(&self) -> &GidTable{
        &self.gid_table
    }
}

unsafe impl Send for IbvDevice{}
unsafe impl Sync for IbvDevice{}

pub enum LookUpBy{
    Address(std::net::IpAddr),
    Name(String),
    None,
}

fn device_lookup(look_up_by: LookUpBy) -> anyhow::Result<(*mut ibv_device, *mut ibv_context, GidTable)>{
    let device_list: *mut *mut ibv_device = unsafe { ibv_get_device_list(null_mut()) };
    if device_list.is_null() {
        return Err(anyhow::anyhow!("No device found"));
    }
    let mut i = 0;
    while !device_list.is_null() {
        let device = unsafe { *device_list.wrapping_add(i) };
        if device.is_null() {
            continue;
        }
        let context = unsafe { ibv_open_device(device) };
        if context == null_mut() {
            continue;
        }
        let device_name: &CStr = unsafe { CStr::from_ptr((*device).name.as_ptr()) }; // Convert array to raw pointer
        let gid_table = get_gid_table(context, device_name.to_str().unwrap())?;
        match look_up_by{
            LookUpBy::Address(addr) => {
                if gid_table.contains(addr){
                    info!("Device found by address");
                    return Ok((device, context, gid_table));
                }
            },
            LookUpBy::Name(ref name) => {

                let name = name.as_str();
                if name == device_name.to_str().unwrap(){
                    info!("Device found by name");
                    return Ok((device, context, gid_table));
                }
            },
            LookUpBy::None => {
                info!("Device found by none");
                return Ok((device, context, gid_table));
            },
        }
        i += 1;
    }
    return Err(anyhow::anyhow!("Device not found"));
}

fn get_gid_table(device_ctx: *mut ibv_context, device_name: &str) -> anyhow::Result<GidTable>{
    let mut device_attr: ibv_device_attr = unsafe { std::mem::zeroed::<ibv_device_attr>() };
    let mut gid_table = GidTable{
        v4_table: BTreeMap::new(),
        v6_table: BTreeMap::new(),
    };
    let ret = unsafe { ibv_query_device(device_ctx, &mut device_attr) };
    if ret != 0 {
        return Err(anyhow::anyhow!("Failed to query device"));
    }
    let num_ports = device_attr.phys_port_cnt;
    for i in 1..=num_ports {
        let mut port_attr: ibv_port_attr = unsafe { std::mem::zeroed::<ibv_port_attr>() };
        let ret = unsafe { ___ibv_query_port(device_ctx, i, &mut port_attr) };
        if ret != 0 {
            return Err(anyhow::anyhow!("Failed to query port"));
        }
        let gid_tbl_len = port_attr.gid_tbl_len;
        for j in 0..gid_tbl_len {
            let mut gid: ibv_gid = unsafe { std::mem::zeroed() };
            unsafe { ibv_query_gid(device_ctx, i, j, &mut gid) };
            let address = if let Some(gid_v6) = gid_to_ipv6_string(gid){
                match gid_v6.to_ipv4(){
                    Some(gid_v4) => {
                        Some(std::net::IpAddr::V4(gid_v4))
                    },
                    None => {
                        let segments = gid_v6.segments();
                        if (segments[0] & 0xffc0) != 0xfe80 {
                            Some(std::net::IpAddr::V6(gid_v6))
                        } else {
                            None
                        }
                    },
                }
            } else {
                None
                    
            };
            if let Some(address) = address{
                match read_gid_type(device_name, i, j)?{
                    GidType::ROCEv2 => {
                        let gid = IbvGid::new(gid);
                        let gid_entry = GidEntry{
                            gid,
                            port: i,
                            gidx: j,
                        };
                        gid_table.add_entry(gid_entry, address);
                        

                    },
                    GidType::RoCEv1 => {

                    },
                }
            }
        }
    }
    Ok(gid_table)
}

fn read_gid_type(device_name: &str, port: u8, gid_index: i32) -> anyhow::Result<GidType> {
    // Construct the file path
    let path = PathBuf::from(format!(
        "/sys/class/infiniband/{}/ports/{}/gid_attrs/types/{}",
        device_name, port, gid_index
    ));

    // Read the file contents
    let gid_type = fs::read_to_string(path).map_err(|_| anyhow::anyhow!("failed to read"))?;

    // Return the contents as a String
    let gid_type = GidType::from_str(gid_type.trim());
    Ok(gid_type)
}

pub fn gid_to_ipv6_string(gid: ibv_gid) -> Option<std::net::Ipv6Addr> {
    unsafe {
        // Access the raw bytes of the gid union
        let raw_gid = gid.raw;
        // check if all bytes are zero
        let mut all_zero = true;
        for i in 0..16{
            if raw_gid[i] != 0{
                all_zero = false;
                break;
            }
        }
        if all_zero{
            return None;
        }

        // Create an Ipv6Addr from the raw bytes
        let ipv6_addr = std::net::Ipv6Addr::new(
            (raw_gid[0] as u16) << 8 | (raw_gid[1] as u16),
            (raw_gid[2] as u16) << 8 | (raw_gid[3] as u16),
            (raw_gid[4] as u16) << 8 | (raw_gid[5] as u16),
            (raw_gid[6] as u16) << 8 | (raw_gid[7] as u16),
            (raw_gid[8] as u16) << 8 | (raw_gid[9] as u16),
            (raw_gid[10] as u16) << 8 | (raw_gid[11] as u16),
            (raw_gid[12] as u16) << 8 | (raw_gid[13] as u16),
            (raw_gid[14] as u16) << 8 | (raw_gid[15] as u16),
        );

        // Convert the Ipv6Addr to a string
        Some(ipv6_addr)
    }
}


pub struct GidTable{
    pub v4_table: BTreeMap<Ipv4Addr, GidEntry>,
    pub v6_table: BTreeMap<Ipv6Addr, GidEntry>,
}

impl GidTable{
    pub fn add_entry(&mut self, gid_entry: GidEntry, address: IpAddr){
        match address{
            IpAddr::V4(v4) => {
                self.v4_table.insert(v4, gid_entry);
            },
            IpAddr::V6(v6) => {
                self.v6_table.insert(v6, gid_entry);
            }
        }
    }
    pub fn get_entry_by_address(&self, address: IpAddr) -> Option<&GidEntry>{
        match address{
            IpAddr::V4(v4) => {
                self.v4_table.get(&v4)
            },
            IpAddr::V6(v6) => {
                self.v6_table.get(&v6)
            }
        }
    }
    
    pub fn get_entry_by_index(&self, index: usize, family: Family) -> Option<(IpAddr, &GidEntry)>{
        match family{
            Family::Inet => {
                self.v4_table.iter().nth(index).map(|(k, v)| {
                    let ip_addr = IpAddr::V4(*k);
                    (ip_addr, v)
                })
            },
            Family::Inet6 => {
                self.v6_table.iter().nth(index).map(|(k, v)| {
                    let ip_addr = IpAddr::V6(*k);
                    (ip_addr, v)
                })
            }
        }
    }

    pub fn contains(&self, address: IpAddr) -> bool{
        match address{
            IpAddr::V4(v4) => {
                self.v4_table.contains_key(&v4)
            },
            IpAddr::V6(v6) => {
                self.v6_table.contains_key(&v6)
            }
        }
    }
    pub fn get_entry_by_family(&self, family: Family) -> Option<(IpAddr, &GidEntry)>{
        match family{
            Family::Inet => {
                self.v4_table.iter().next().map(|(k, v)| {
                    let ip_addr = IpAddr::V4(*k);
                    (ip_addr, v)
                })
            },
            Family::Inet6 => {
                self.v6_table.iter().next().map(|(k, v)| {
                    let ip_addr = IpAddr::V6(*k);
                    (ip_addr, v)
                })
            }
        }
    }
}

pub struct GidEntry{
    gid: IbvGid,
    port: u8,
    gidx: i32,
}
impl GidEntry{
    pub fn subnet_id(&self) -> u64{
        unsafe { self.gid.inner.global.subnet_prefix }
    }
    pub fn interface_id(&self) -> u64{
        unsafe { self.gid.inner.global.interface_id }
    }
    pub fn gidx(&self) -> i32{
        self.gidx
    }
    pub fn port(&self) -> u8{
        self.port
    }
}
enum GidType{
    ROCEv2,
    RoCEv1,
}
impl GidType{
    fn from_str(s: &str) -> GidType{
        match s{
            "RoCE v2" => GidType::ROCEv2,
            "IB/RoCE v1" => GidType::RoCEv1,
            _ => GidType::ROCEv2,
        }
    }
}

pub struct IbvContext{
    inner: Box<*mut ibv_context>,
}

impl IbvContext{
    pub fn from_device(device: IbvDevice) -> Self{
        let context = unsafe{ ibv_open_device(device.as_ptr()) };
        let inner = Box::new(context);
        IbvContext{
            inner,
        }
    }
    pub fn from_context(context: *mut ibv_context) -> Self{
        let inner = Box::new(context);
        IbvContext{
            inner,
        }
    }
    pub fn as_ptr(&self) -> *mut ibv_context {
        *self.inner
    }
}

impl Drop for IbvContext{
    fn drop(&mut self){
        info!("Destroying context");
        unsafe{ ibv_close_device(self.as_ptr()) };
    }
}

unsafe impl Send for IbvContext{}
unsafe impl Sync for IbvContext{}

pub struct IbvMr{
    inner: Box<*mut ibv_mr>,
}

impl IbvMr{
    pub fn new(pd: &IbvPd, addr: *mut u8, length: usize, access: i32) -> Self{
        let addr = addr as *mut std::ffi::c_void;
        info!("access: {}", access.clone() as i32);
        let mr = unsafe{ ibv_reg_mr(pd.as_ptr(), addr, length, access) };
        let mr_addr = unsafe{ (*mr).addr as u64 };
        let mr_rkey = unsafe{ (*mr).rkey };
        let mr_lkey = unsafe{ (*mr).lkey };
        info!("mr addr: {}, rkey: {}, lkey: {}", mr_addr, mr_rkey, mr_lkey);
        let inner = Box::new(mr);
        IbvMr{
            inner,
        }
    }
    pub fn as_ptr(&self) -> *mut ibv_mr{
        *self.inner
    }
    pub fn addr(&self) -> u64{
        unsafe{ (*self.as_ptr()).addr as u64 }
    }
    pub fn length(&self) -> usize{
        unsafe{ (*self.as_ptr()).length as usize }
    }
    pub fn lkey(&self) -> u32{
        unsafe{ (*self.as_ptr()).lkey }
    }
    pub fn rkey(&self) -> u32{
        unsafe{ (*self.as_ptr()).rkey }
    }
}

impl Drop for IbvMr{
    fn drop(&mut self){
        info!("Destroying MR");
        unsafe{ ibv_dereg_mr(self.as_ptr()) };
    }
}

unsafe impl Send for IbvMr{}
unsafe impl Sync for IbvMr{}

#[derive(Clone)]
pub enum IbvAccessFlags{
    LocalWrite = 1,
    RemoteWrite = 2,
    RemoteRead = 4,
    RemoteAtomic = 8,
    MwBind = 16,
    ZeroBased = 32,
    OnDemand = 64,
    HugeTlb = 128,
}

impl IbvAccessFlags{
    pub fn as_i32(&self) -> i32{
        self.clone() as i32
    }
}

impl Into <i32> for IbvAccessFlags{
    fn into(self) -> i32{
        self as i32
    }
}

impl From <i32> for IbvAccessFlags{
    fn from(val: i32) -> Self{
        match val{
            1 => IbvAccessFlags::LocalWrite,
            2 => IbvAccessFlags::RemoteWrite,
            4 => IbvAccessFlags::RemoteRead,
            8 => IbvAccessFlags::RemoteAtomic,
            16 => IbvAccessFlags::MwBind,
            32 => IbvAccessFlags::ZeroBased,
            64 => IbvAccessFlags::OnDemand,
            128 => IbvAccessFlags::HugeTlb,
            _ => IbvAccessFlags::LocalWrite,
        }
    }
}

impl BitOr for IbvAccessFlags{
    type Output = i32;
    fn bitor(self, rhs: Self) -> Self::Output{
        self as i32 | rhs as i32
    }
}
pub struct IbvSge{
    inner: ibv_sge,
}

impl IbvSge{
    pub fn new(addr: u64, length: u32, lkey: u32) -> Self{
        let sge = ibv_sge{
            addr,
            length,
            lkey,
        };
        IbvSge{
            inner: sge
        }
    }
    pub fn as_ptr(&self) -> *mut ibv_sge{
        &self.inner as *const _ as *mut _
    }
}

unsafe impl Send for IbvSge{}
unsafe impl Sync for IbvSge{}

pub struct IbvRecvWr{
    inner: ibv_recv_wr,
    next: Option<Box<IbvRecvWr>>,
}

impl IbvRecvWr{
    pub fn new(id: u64, sg_list: IbvSge, num_sge: i32) -> Self{
        let mut wr: ibv_recv_wr = unsafe { std::mem::zeroed() };  // Create a zeroed ibv_recv_wr
        wr.wr_id = id;
        wr.sg_list = sg_list.as_ptr();
        wr.num_sge = num_sge;

        IbvRecvWr{
            inner: wr,
            next: None,
        }
    }

    pub fn set_next(&mut self, next: IbvRecvWr){
        self.next = Some(Box::new(next));
        unsafe {
            (*self.as_ptr()).next = self.next.as_ref().map_or(std::ptr::null_mut(), |n| n.as_ptr());
        }
    }

    pub fn as_ptr(&self) -> *mut ibv_recv_wr {
        &self.inner as *const _ as *mut _
    }
}

unsafe impl Send for IbvRecvWr{}
unsafe impl Sync for IbvRecvWr{}

pub struct IbvSendWr{
    inner: ibv_send_wr,
    next: Option<Box<IbvSendWr>>,
}

impl IbvSendWr{
    pub fn new(id: u64,
        sg_list: IbvSge,
        num_sge: i32,
        opcode: IbvWrOpcode,
        send_flags: i32,
        remote_addr: u64,
        rkey: u32,
    ) -> Self{
        let mut wr: ibv_send_wr = unsafe { std::mem::zeroed() };  // Create a zeroed ibv_send_wr
        wr.wr_id = id;
        wr.sg_list = sg_list.as_ptr();
        wr.num_sge = num_sge;
        wr.opcode = opcode.get();
        wr.send_flags = send_flags as u32;
        wr.wr.rdma.remote_addr = remote_addr;
        wr.wr.rdma.rkey = rkey;

        IbvSendWr {
            inner: wr,
            next: None,
        }
    }

    pub fn set_next(&mut self, next: IbvSendWr){
        self.next = Some(Box::new(next));
        unsafe {
            (*self.as_ptr()).next = self.next.as_ref().map_or(std::ptr::null_mut(), |n| n.as_ptr());
        }
    }

    pub fn as_ptr(&self) -> *mut ibv_send_wr {
        &self.inner as *const _ as *mut _
    }
}

unsafe impl Send for IbvSendWr{}
unsafe impl Sync for IbvSendWr{}

pub enum IbvWrOpcode{
    RdmaWrite,
    RdmaWriteWithImm,
    Send,
    SendWithImm,
    RdmaRead,
    AtomicCmpAndSwp,
    AtomicFetchAndAdd,
    LocalInv,
    BindMw,
    SendWithInv,
    Tso,
    Driver1,
    AtomicWrite,
}

impl IbvWrOpcode{
    pub fn get(&self) -> ibv_wr_opcode::Type{
        match self{
            IbvWrOpcode::RdmaWrite => ibv_wr_opcode::IBV_WR_RDMA_WRITE,
            IbvWrOpcode::RdmaWriteWithImm => ibv_wr_opcode::IBV_WR_RDMA_WRITE_WITH_IMM,
            IbvWrOpcode::Send => ibv_wr_opcode::IBV_WR_SEND,
            IbvWrOpcode::SendWithImm => ibv_wr_opcode::IBV_WR_SEND_WITH_IMM,
            IbvWrOpcode::RdmaRead => ibv_wr_opcode::IBV_WR_RDMA_READ,
            IbvWrOpcode::AtomicCmpAndSwp => ibv_wr_opcode::IBV_WR_ATOMIC_CMP_AND_SWP,
            IbvWrOpcode::AtomicFetchAndAdd => ibv_wr_opcode::IBV_WR_ATOMIC_FETCH_AND_ADD,
            IbvWrOpcode::LocalInv => ibv_wr_opcode::IBV_WR_LOCAL_INV,
            IbvWrOpcode::BindMw => ibv_wr_opcode::IBV_WR_BIND_MW,
            IbvWrOpcode::SendWithInv => ibv_wr_opcode::IBV_WR_SEND_WITH_INV,
            IbvWrOpcode::Tso => ibv_wr_opcode::IBV_WR_TSO,
            IbvWrOpcode::Driver1 => ibv_wr_opcode::IBV_WR_DRIVER1,
            IbvWrOpcode::AtomicWrite => ibv_wr_opcode::IBV_WR_ATOMIC_WRITE,
        }
    }
}

pub enum IbvSendFlags{
    Fence,
    Signaled,
    Solicited,
    Inline,
    IpCsum,
}

impl IbvSendFlags{
    pub fn get(&self) -> u32{
        match self{
            IbvSendFlags::Fence => ibv_send_flags::IBV_SEND_FENCE.0,
            IbvSendFlags::Signaled => ibv_send_flags::IBV_SEND_SIGNALED.0,
            IbvSendFlags::Solicited => ibv_send_flags::IBV_SEND_SOLICITED.0,
            IbvSendFlags::Inline => ibv_send_flags::IBV_SEND_INLINE.0,
            IbvSendFlags::IpCsum => ibv_send_flags::IBV_SEND_IP_CSUM.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Family{
    Inet,
    Inet6,
}

pub enum Hints{
    AddressFamily(Family),
    Address(IpAddr),
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MrMetadata{
    pub address: u64,
    pub rkey: u32,
    pub length: u64,
}

impl MrMetadata{
    pub const SIZE: usize = std::mem::size_of::<MrMetadata>();
    pub fn addr(&self) -> *mut u8{
        &self as *const _ as *mut u8
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct QpMetadata{
    pub subnet_id: u64,
    pub interface_id: u64,
    pub psn: u32,
    pub qpn: u32,
}

#[derive(Serialize, Deserialize)]
pub struct SocketComm{
    pub command: SocketCommCommand,
}

#[derive(Serialize, Deserialize)]
pub enum SocketCommCommand{
    Mr(MrMetadata),
    InitQp(u32, Family),
    ConnectQp(QpMetadata),
    Stop,
}
