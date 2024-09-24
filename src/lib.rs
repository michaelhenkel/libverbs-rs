use std::{collections::BTreeMap, ffi::{c_void, CStr}, fs, net::{IpAddr, Ipv4Addr, Ipv6Addr}, ops::BitOr, os::fd::RawFd, path::PathBuf, pin::Pin, ptr::{self, null_mut}, sync::Arc};
use rdma_sys::*;
use serde::{Deserialize, Serialize};
use std::sync::Once;
use env_logger::Env;
use std::fmt::Debug;
//use log::LevelFilter;

static INIT: Once = Once::new();
const BATCH_SIZE: usize = 2000;
const MAX_MESSAGE_SIZE: u64 = 1024 * 1024;
pub const SLOT_COUNT: usize = 256;

pub fn initialize_logger() {
    INIT.call_once(|| {
        env_logger::Builder::from_env(Env::default().default_filter_or("info"))
            //.filter_module("my_library", LevelFilter::Info)
            .init();
    });
}

pub mod sender;
pub mod receiver;

#[derive(Clone)]
pub enum QpMode{
    Single,
    Multi,
}
#[derive(Clone)]
#[allow(dead_code)]
pub struct IbvQp{
    inner: Box<*mut ibv_qp>,
    recv_cq: Arc<IbvCq>,
    send_cq: Arc<IbvCq>,
    event_channel: Arc<IbvCompChannel>,
    rdma_port_num: u8,
    remote_qp_metadata: Option<QpMetadata>,
    psn: u32,
    gidx: i32,
    port: u8,
    rate_limit: Option<u32>,
    context: Arc<IbvContext>,
    local_gid: String,
    remote_gid: String,
    hca_name: String,
}

impl IbvQp{
    pub fn new(pd: Arc<IbvPd>, context: Arc<IbvContext>, gidx: i32, port: u8, rate_limit: Option<u32>) -> Self{
        let comp_channel = Arc::new(IbvCompChannel::new(&context));
        let recv_cq = Arc::new(IbvCq::new(&context, 4096, &comp_channel, 0));
        let send_cq = Arc::clone(&recv_cq);
        let mut qp_init_attr = ibv_qp_init_attr {
            qp_context: null_mut(),
            send_cq: recv_cq.as_ptr(),
            recv_cq: send_cq.as_ptr(),
            srq: null_mut(),
            cap: ibv_qp_cap {
                max_send_wr: 4096,
                max_recv_wr: 4096,
                max_send_sge: 1,
                max_recv_sge: 1,
                max_inline_data: 64,
            },
            qp_type: ibv_qp_type::IBV_QPT_RC,
            sq_sig_all: 0,
        };
        let qp = unsafe{ ibv_create_qp(pd.as_ptr(), &mut qp_init_attr) };
        let inner = Box::new(qp);
        let psn = rand::random::<u32>() & 0xffffff;
        IbvQp{
            inner,
            recv_cq,
            send_cq,
            event_channel: comp_channel,
            rdma_port_num: 0,
            remote_qp_metadata: None,
            psn,
            gidx,
            port,
            rate_limit,
            context,
            local_gid: String::new(),
            remote_gid: String::new(),
            hca_name: String::new(),
        }
    }
    pub fn local_gid(&self) -> String{
        self.local_gid.clone()
    }
    pub fn remote_gid(&self) -> String{
        self.remote_gid.clone()
    }
    pub fn hca_name(&self) -> String{
        self.hca_name.clone()
    }
    pub fn event_channel(&self) -> Arc<IbvCompChannel>{
        Arc::clone(&self.event_channel)
    }
    pub fn recv_cq(&self) -> Arc<IbvCq>{
        Arc::clone(&self.recv_cq)
    }
    pub fn send_cq(&self) -> Arc<IbvCq>{
        Arc::clone(&self.send_cq)
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
    pub fn init(&mut self, port: u8) -> anyhow::Result<()>{
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
        self.rdma_port_num = port;
        Ok(())
    }
    pub fn connect(&mut self, remote_qp_metadata: &QpMetadata) -> anyhow::Result<()>{
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
        let remote_address = gid_to_ipv6_string(remote_gid).unwrap();
        self.remote_gid = remote_address.to_string();
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
        qp_attr.timeout = 7;
        qp_attr.min_rnr_timer = 7;
        qp_attr.ah_attr.sl = 0;
        qp_attr.ah_attr.src_path_bits = 0;
        qp_attr.ah_attr.port_num = port;
        qp_attr.ah_attr.dlid = 0;
        qp_attr.ah_attr.grh.dgid = remote_gid;
        qp_attr.ah_attr.is_global = 1;
        qp_attr.ah_attr.grh.sgid_index = gidx as u8;
        qp_attr.ah_attr.grh.hop_limit = 10;
        //qp_attr.ah_attr.grh.flow_label = 666;
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
        qp_attr.timeout = 7;
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
        self.remote_qp_metadata = Some(remote_qp_metadata.clone());
        Ok(())
    }
    pub fn recover_qp(&mut self) -> anyhow::Result<()>{
        println!("Recovering QP");
        let mut qp_attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
        qp_attr.qp_state = ibv_qp_state::IBV_QPS_RESET;
        let qp_attr_mask = ibv_qp_attr_mask::IBV_QP_STATE;
        let ret = unsafe { ibv_modify_qp(self.as_ptr(), &mut qp_attr, qp_attr_mask.0 as i32) };
        if ret != 0 {
            return Err(anyhow::anyhow!("Failed to modify QP to RESET"));
        }
        let port_num = self.rdma_port_num;
        let remote_qp_metadata = self.remote_qp_metadata.as_ref().unwrap().clone();
        self.init(port_num)?;
        self.connect(&remote_qp_metadata)?;
        Ok(())
    }
    pub fn ibv_post_send(&mut self, send_wr: *mut ibv_send_wr) -> anyhow::Result<()>{
        let mut bad_wr: *mut ibv_send_wr = ptr::null_mut();
        let ret = unsafe{ ibv_post_send(self.as_ptr(), send_wr, &mut bad_wr) };
        if ret != 0 {
            if self.qp_error()? {
                self.recover_qp()?;
            } else {
                return Err(anyhow::anyhow!("Failed to post send"));
            }
        }
        Ok(())
    }
    pub fn ibv_post_recv(&mut self, recv_wr: IbvRecvWr) -> anyhow::Result<()>{
        let mut bad_wr: *mut ibv_recv_wr = ptr::null_mut();
        let ret = unsafe { ibv_post_recv(self.as_ptr(), recv_wr.as_ptr(), &mut bad_wr) };
        if ret != 0 {
            if self.qp_error()? {
                self.recover_qp()?;
            } else {
                return Err(anyhow::anyhow!("Failed to post recv"));
            }
        }
        Ok(())
    }
    pub fn wait_for_event(&self) -> anyhow::Result<()>{
        let event_channel = self.event_channel.as_ptr();
        let mut cq = self.recv_cq.as_ptr();
        let mut context = null_mut();
        let ret = unsafe{ ibv_get_cq_event(event_channel, &mut cq, &mut context) };
        if ret != 0 {
            return Err(anyhow::anyhow!("Failed to get cq event"));
        }
        let ret = unsafe{ ibv_req_notify_cq(cq, 0) };
        if ret != 0 {
            return Err(anyhow::anyhow!("Failed to request notify cq"));
        }
        let mut wc = unsafe { std::mem::zeroed::<ibv_wc>() };
        let ret = unsafe{ ibv_poll_cq(cq, 1, &mut wc) };
        if ret != 1 {
            return Err(anyhow::anyhow!("Failed to poll cq"));
        }
        if wc.status != ibv_wc_status::IBV_WC_SUCCESS {
            return Err(anyhow::anyhow!("Failed to get success status"));
        }
        Ok(())
    }
    pub fn wait_for_send_event(&self) -> anyhow::Result<()>{
        let event_channel = self.event_channel.as_ptr();
        let mut cq = self.send_cq.as_ptr();
        let mut context = null_mut();
        let ret = unsafe{ ibv_get_cq_event(event_channel, &mut cq, &mut context) };
        if ret != 0 {
            return Err(anyhow::anyhow!("Failed to get cq event"));
        }
        let ret = unsafe{ ibv_req_notify_cq(cq, 0) };
        if ret != 0 {
            return Err(anyhow::anyhow!("Failed to request notify cq"));
        }
        let mut wc = unsafe { std::mem::zeroed::<ibv_wc>() };
        let ret = unsafe{ ibv_poll_cq(cq, 1, &mut wc) };
        if ret != 1 {
            return Err(anyhow::anyhow!("Failed to poll cq"));
        }
        unsafe { ibv_ack_cq_events(self.send_cq().as_ptr(), 1) };
        if wc.status != ibv_wc_status::IBV_WC_SUCCESS {
            return Err(anyhow::anyhow!("Failed to get success status"));
        }
        Ok(())
    }
    pub fn state(&self) -> anyhow::Result<u32>{
        let state = unsafe{ (*self.as_ptr()).state };
        Ok(state)
    }
    pub fn qp_attr(&self, qp_attr_mask: ibv_qp_attr_mask) -> anyhow::Result<ibv_qp_attr>{
        let mut qp_attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
        let mut qp_init_attr = unsafe { std::mem::zeroed::<ibv_qp_init_attr>() };
        let ret = unsafe { ibv_query_qp(self.as_ptr(), &mut qp_attr, qp_attr_mask.0 as i32, &mut qp_init_attr) };
        if ret != 0 {
            return Err(anyhow::anyhow!("Failed to query QP"));
        }
        Ok(qp_attr)
    }
    pub fn qp_error(&self) -> anyhow::Result<bool>{
        let state = self.state()?;
        if state == 6 {
            return Ok(true);
        }
        Ok(false)
    }
        
    pub fn destroy(&self){
        unsafe{ ibv_destroy_qp(self.as_ptr()) };
    }
    pub fn poll_complete(&mut self, expected_completions: usize, _opcode_type: IbvWcOpcode, func: Option<fn()>) -> anyhow::Result<(usize, Vec<(u64, Option<u32>)>)>{
        let cq = self.recv_cq().as_ptr();
        let mut wc_vec: Vec<ibv_wc> = Vec::with_capacity(expected_completions);
        let wc_ptr = wc_vec.as_mut_ptr();
        let wc_done = unsafe { ibv_poll_cq(cq, expected_completions as i32, wc_ptr) };
        if wc_done < 0 {
            if self.qp_error()? {
                self.recover_qp()?;
            } else {
                return Err(anyhow::anyhow!("Failed to poll cq"));
            }
        }
        unsafe { wc_vec.set_len(wc_done as usize) };
        let mut wr_id_list = Vec::new();
        for i in 0..wc_done{
            let wc = wc_ptr.wrapping_add(i as usize);
            if wc.is_null(){
                println!("wc is null");
            }
            let status = unsafe { (*wc).status };
            let opcode = unsafe { (*wc).opcode };
            let wr_id = unsafe { (*wc).wr_id };
            let imm = if opcode == 129{
                    let imm = unsafe { (*wc).imm_data_invalidated_rkey_union.imm_data };
                    Some(imm)

            } else {
                None
            };
            
            wr_id_list.push((wr_id, imm));
            if status != ibv_wc_status::IBV_WC_SUCCESS {
                return Err(anyhow::anyhow!(format!("wc status {} wrong, expected {}", status, ibv_wc_status::IBV_WC_SUCCESS).to_string()));
            }  
        }
        Ok((wc_done as usize, wr_id_list))
    }
    
    pub fn event_complete(&self, iterations: usize, opcode_type: IbvWcOpcode, batch_size: Option<usize>) -> anyhow::Result<(usize, Vec<(u64, Option<u32>)>)>{
        let batch_size = match batch_size{
            Some(size) => size,
            None => BATCH_SIZE,
        };
        let mut wc_vec: Vec<ibv_wc> = Vec::with_capacity(batch_size);
        let wc_ptr = wc_vec.as_mut_ptr();
        let mut total_wc: i32 = 0;
        let mut context = ptr::null::<c_void>() as *mut _;
        let solicited_only = 0;
        let mut cq = self.recv_cq().as_ptr();
        loop {
            let ret = unsafe { ibv_poll_cq(cq, batch_size as i32, wc_ptr.wrapping_add(total_wc as usize)) };
            if ret < 0 {
                return Err(anyhow::anyhow!("Failed to poll cq"));
            }
            total_wc += ret;
            if total_wc >= iterations as i32 {
                break;
            }
            if total_wc < iterations as i32 {
                let ret = unsafe { ibv_req_notify_cq(cq, solicited_only) };
                if ret != 0 {
                    return Err(anyhow::anyhow!("Failed to request notify cq"));
                }
                let ret = unsafe { ibv_get_cq_event(self.event_channel().as_ptr(), &mut cq, &mut context) };
                if ret != 0 {
                    return Err(anyhow::anyhow!("Failed to get cq event"));
                }
                unsafe { ibv_ack_cq_events(self.recv_cq().as_ptr(), 1) };
                let ret = unsafe { ibv_poll_cq(cq, batch_size as i32, wc_ptr.wrapping_add(total_wc as usize)) };
                if ret < 0 {
                    return Err(anyhow::anyhow!("Failed to poll cq"));
                }
                total_wc += ret;
                if total_wc >= iterations as i32 {
                    break;
                }
            }
        }
        let mut wr_id_list = Vec::new();
        for i in 0..total_wc{
            let wc = wc_ptr.wrapping_add(i as usize);
            let status = unsafe { (*wc).status };
            let wr_id = unsafe { (*wc).wr_id };
            let imm = match opcode_type{
                IbvWcOpcode::RecvRdmaWithImm => {
                    let imm = unsafe { (*wc).imm_data_invalidated_rkey_union.imm_data };
                    Some(imm)
                },
                _ => { None }
            };
            wr_id_list.push((wr_id, imm));
            if status != ibv_wc_status::IBV_WC_SUCCESS {
                return Err(anyhow::anyhow!(format!("wc status {} wrong, expected {}", status, ibv_wc_status::IBV_WC_SUCCESS).to_string()));
            }
        }
        Ok((total_wc as usize, wr_id_list))
    }
}

impl Debug for IbvQp{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IbvQp {{ qpn: {:?}, psn: {:?} }}", self.qp_num(), self.psn())
    }
}



#[derive(Clone, Debug)]
pub enum SendRecv{
    Send,
    Recv,
}

unsafe impl Send for IbvQp{}
unsafe impl Sync for IbvQp{}

pub struct IbvEventType(pub ibv_event_type);

impl IbvEventType {
    // Convert the event type to a human-readable string
    pub fn to_str(&self) -> &'static str {
        match self.0 {
            ibv_event_type::IBV_EVENT_CQ_ERR => "Completion Queue Error",
            ibv_event_type::IBV_EVENT_QP_FATAL => "Queue Pair Fatal Error",
            ibv_event_type::IBV_EVENT_QP_REQ_ERR => "Queue Pair Request Error",
            ibv_event_type::IBV_EVENT_QP_ACCESS_ERR => "Queue Pair Access Error",
            ibv_event_type::IBV_EVENT_COMM_EST => "Communication Established",
            ibv_event_type::IBV_EVENT_SQ_DRAINED => "Send Queue Drained",
            ibv_event_type::IBV_EVENT_PATH_MIG => "Path Migration Successful",
            ibv_event_type::IBV_EVENT_PATH_MIG_ERR => "Path Migration Error",
            ibv_event_type::IBV_EVENT_DEVICE_FATAL => "Device Fatal Error",
            ibv_event_type::IBV_EVENT_PORT_ACTIVE => "Port Active",
            ibv_event_type::IBV_EVENT_PORT_ERR => "Port Error",
            ibv_event_type::IBV_EVENT_LID_CHANGE => "LID Change",
            ibv_event_type::IBV_EVENT_PKEY_CHANGE => "P_Key Change",
            ibv_event_type::IBV_EVENT_SM_CHANGE => "SM Change",
            ibv_event_type::IBV_EVENT_SRQ_ERR => "Shared Receive Queue Error",
            ibv_event_type::IBV_EVENT_SRQ_LIMIT_REACHED => "SRQ Limit Reached",
            ibv_event_type::IBV_EVENT_QP_LAST_WQE_REACHED => "Last WQE Reached",
            ibv_event_type::IBV_EVENT_CLIENT_REREGISTER => "Client Reregister",
            ibv_event_type::IBV_EVENT_GID_CHANGE => "GID Table Change",
            ibv_event_type::IBV_EVENT_WQ_FATAL => "Work Queue Fatal Error",
        }
    }

    // Check if the event represents a fatal error
    pub fn is_fatal(&self) -> bool {
        matches!(
            self.0,
            ibv_event_type::IBV_EVENT_QP_FATAL
                | ibv_event_type::IBV_EVENT_DEVICE_FATAL
                | ibv_event_type::IBV_EVENT_WQ_FATAL
        )
    }

    // Example method: Check if the event is related to a QP
    pub fn is_qp_related(&self) -> bool {
        matches!(
            self.0,
            ibv_event_type::IBV_EVENT_QP_FATAL
                | ibv_event_type::IBV_EVENT_QP_REQ_ERR
                | ibv_event_type::IBV_EVENT_QP_ACCESS_ERR
                | ibv_event_type::IBV_EVENT_QP_LAST_WQE_REACHED
        )
    }
}

// Implement From for easy conversion between ibv_event_type and IbvEventType
impl From<ibv_event_type> for IbvEventType {
    fn from(event: ibv_event_type) -> Self {
        IbvEventType(event)
    }
}

impl From<IbvEventType> for ibv_event_type {
    fn from(wrapper: IbvEventType) -> Self {
        wrapper.0
    }
}

impl Debug for IbvEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IbvEventType({})", self.to_str())
    }
}

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
    pub fn new(context: &Arc<IbvContext>, cqe: i32, channel: &Arc<IbvCompChannel>, comp_vector: i32) -> Self{
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
    pub fn destroy(&self){
        unsafe{ ibv_destroy_cq(self.as_ptr()) };
    }
}

/*
impl Drop for IbvCq{
    fn drop(&mut self){
        unsafe{ ibv_destroy_cq(self.as_ptr()) };
    }
}
*/

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
    pub fn destroy(&self){
        unsafe{ ibv_destroy_comp_channel(self.as_ptr()) };
    }
}

/*
impl Drop for IbvCompChannel{
    fn drop(&mut self){
        info!("Destroying comp channel");
        unsafe{ ibv_destroy_comp_channel(self.as_ptr()) };
    }
}
*/

unsafe impl Send for IbvCompChannel{}
unsafe impl Sync for IbvCompChannel{}

pub struct IbvPd{
    inner: Box<*mut ibv_pd>,
}

impl IbvPd{
    pub fn new(context: Arc<IbvContext>) -> Self{
        let pd = unsafe{ ibv_alloc_pd(context.as_ptr()) };
        let inner = Box::new(pd);
        IbvPd{
            inner,
        }
    }
    pub fn as_ptr(&self) -> *mut ibv_pd {
        *self.inner
    }
    pub fn destroy(&self){
        unsafe{ ibv_dealloc_pd(self.as_ptr()) };
    }
}

/*
impl Drop for IbvPd{
    fn drop(&mut self){
        info!("Destroying PD");
        unsafe{ ibv_dealloc_pd(self.as_ptr()) };
    }
}
*/

unsafe impl Send for IbvPd{}
unsafe impl Sync for IbvPd{}
#[derive(Clone)]
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
#[derive(Clone)]
pub struct IbvDevice{
    inner: Box<*mut ibv_device>,
    gid_table: GidTable,
    context: Arc<IbvContext>,
    name: String,
}

impl IbvDevice{
    pub fn new(look_up_by: LookUpBy) -> anyhow::Result<Self>{
        let (device, context, gid_table, name) = device_lookup(look_up_by)?;
        let context = Arc::new(IbvContext::from_context(context));
        let inner = Box::new(device);
        Ok(IbvDevice{
            inner,
            gid_table,
            context,
            name,
        })
    }
    pub fn as_ptr(&self) -> *mut ibv_device {
        *self.inner
    }
    pub fn gid_table(&self) -> &GidTable{
        &self.gid_table
    }
    pub fn context(&self) -> Arc<IbvContext>{
        Arc::clone(&self.context)
    }
    /*
    pub fn destroy(&self){
        unsafe{ ibv_close_device(self.context.as_ptr()) };
    }
    */
}

impl Drop for IbvDevice {
    fn drop(&mut self) {
        //unsafe { ibv_close_device(self.context.as_ptr()) };
        // Do not free `self.inner` because it's not owned by Rust's allocator
    }
}

unsafe impl Send for IbvDevice{}
unsafe impl Sync for IbvDevice{}

pub enum LookUpBy{
    Address(std::net::IpAddr),
    Name(String),
    None,
}

fn device_lookup(look_up_by: LookUpBy) -> anyhow::Result<(*mut ibv_device, *mut ibv_context, GidTable, String)>{
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
                    return Ok((device, context, gid_table, device_name.to_str().unwrap().to_string()));
                }
            },
            LookUpBy::Name(ref name) => {

                let name = name.as_str();
                if name == device_name.to_str().unwrap(){
                    return Ok((device, context, gid_table, device_name.to_str().unwrap().to_string()));
                }
            },
            LookUpBy::None => {
                return Ok((device, context, gid_table, device_name.to_str().unwrap().to_string()));
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

#[derive(Clone)]
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
#[derive(Clone)]
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
    pub fn destroy(&self){
        unsafe{ ibv_close_device(self.as_ptr()) };
    }
}

/*
impl Drop for IbvContext{
    fn drop(&mut self){
        info!("Destroying context");
        unsafe{ ibv_close_device(self.as_ptr()) };
    }
}
*/

unsafe impl Send for IbvContext{}
unsafe impl Sync for IbvContext{}

#[derive(Clone)]
pub struct IbvMr{
    inner: Box<*mut ibv_mr>,
    addr: u64,
    length: usize,
    lkey: u32,
    rkey: u32,
}

impl IbvMr{
    pub fn new(pd: Arc<IbvPd>, addr: *mut c_void, length: usize, access: i32) -> Self{
        //let mr = unsafe{ ibv_reg_mr(pd.as_ptr(), addr, length, access) };
        let mr = unsafe{ ibv_reg_mr_iova2(pd.as_ptr(), addr, length, addr as u64, access as u32) };
        let _addr = unsafe { (*mr).addr };
        let _rkey = unsafe { (*mr).rkey };
        let _lkey = unsafe { (*mr).lkey };
        let _len = unsafe { (*mr).length };
        let inner = Box::new(mr);
        IbvMr{
            inner,
            addr: _addr as u64,
            length: _len as usize,
            lkey: _lkey,
            rkey: _rkey,
        }
    }
    pub fn as_ptr(&self) -> *mut ibv_mr{
        *self.inner
    }
    
    pub fn addr(&self) -> u64{
        //unsafe{ (*self.as_ptr()).addr as u64 }
        self.addr
    }
    pub fn length(&self) -> usize{
        //unsafe{ (*self.as_ptr()).length as usize }
        self.length
    }
    pub fn lkey(&self) -> u32{
        //unsafe{ (*self.as_ptr()).lkey }
        self.lkey
    }
    pub fn rkey(&self) -> u32{
        //unsafe{ (*self.as_ptr()).rkey }
        self.rkey
    }
    
    pub fn destroy(&self){
        unsafe{ ibv_dereg_mr(self.as_ptr()) };
    }
    pub fn convert<T>(&self) -> *mut T{
        self.addr as *mut T
    }
}

/*
impl Drop for IbvMr{
    fn drop(&mut self){
        unsafe{ ibv_dereg_mr(self.as_ptr()) };
    }
}
*/

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

#[derive(Clone)]
pub struct IbvRecvWr{
    inner: ibv_recv_wr,
    next: Option<Box<IbvRecvWr>>,
}

impl IbvRecvWr{
    pub fn new(mr: Option<&IbvMr>, wr_id: Option<u64>) -> Self{
        let mut wr: ibv_recv_wr = unsafe { std::mem::zeroed() };  // Create a zeroed ibv_recv_wr
        wr.wr_id = wr_id.unwrap_or(0);
        wr.num_sge = 1;
        if let Some(mr) = mr{
            let addr = mr.addr();
            let length = mr.length();
            let lkey = mr.lkey();
            let sge = Box::new(ibv_sge {
                addr,
                length: length as u32,
                lkey,
            });
    
            // Convert the Box into a raw pointer
            let sge_ptr = Box::into_raw(sge);
            wr.sg_list = sge_ptr;
        } else {
            let sge = Box::new(ibv_sge {
                addr: 0,
                length: 0,
                lkey: 0,
            });
            let sge_ptr = Box::into_raw(sge);
            wr.sg_list = sge_ptr;
        }
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

pub struct IbvSendWrList(Vec<IbvSendWr>);
impl IbvSendWrList{
    pub fn new(
        local_addr: u64,
        lkey: u32,
        remote_addr: u64,
        rkey: u32,
        size: u64,
        offset: u64,
        opcode: IbvWrOpcode,
        signaled: bool,
        wr_id: Option<u64>,
        last_wr_with_imm: bool,
        qps: u64,
    ) -> Self{
        let local_addr = local_addr + offset;
        let mut remote_addr = remote_addr + offset;
        let mut _wr_id = match wr_id {
            Some(wr_id) => wr_id,
            None => local_addr,
        };

        let volume_size = size;
        let mut remaining_volume = volume_size;
        let mut data_addr = local_addr;
        let lkey = lkey;
        let mut wr_list = Vec::new();
        for i in 0..qps {
            let mut last_wr: *mut ibv_send_wr = ptr::null_mut();
            let mut first_wr: *mut ibv_send_wr = ptr::null_mut();
            let mut qp_remaining_volume = (volume_size + qps - 1 - i) / qps;
            while qp_remaining_volume > 0 && remaining_volume > 0 {
                let message = qp_remaining_volume.min(MAX_MESSAGE_SIZE);
                let is_last_message_for_qp = qp_remaining_volume == message;
                let sge = ibv_sge{
                    addr: data_addr,
                    length: message as u32,
                    lkey,
                };
                let sge = Box::new(sge);
                let sge_ptr: *mut ibv_sge = Box::into_raw(sge);
                let mut wr = unsafe { std::mem::zeroed::<ibv_send_wr>() };
                wr.sg_list = sge_ptr;
                wr.num_sge = 1;
                wr.opcode = opcode.get();
                wr.wr.rdma.remote_addr = remote_addr;
                wr.wr.rdma.rkey = rkey;
                if is_last_message_for_qp && signaled {
                    wr.send_flags = ibv_send_flags::IBV_SEND_SIGNALED.0;
                } else {
                    wr.send_flags = 0;
                }
                if is_last_message_for_qp && last_wr_with_imm {
                    wr.opcode = ibv_wr_opcode::IBV_WR_RDMA_WRITE_WITH_IMM;
                    wr.imm_data_invalidated_rkey_union.imm_data = message as u32;
                }
                wr.wr_id = _wr_id;
                let wr = Box::new(wr);
                let wr_ptr: *mut ibv_send_wr = Box::into_raw(wr);
                if !last_wr.is_null() {
                    unsafe {
                        (*last_wr).next = wr_ptr;
                    }
                } else {
                    first_wr = wr_ptr;
                }
                last_wr = wr_ptr;
                data_addr += message as u64;
                remote_addr += message as u64;
                remaining_volume -= message;
                qp_remaining_volume -= message;
            }
            wr_list.push(IbvSendWr{ inner: first_wr });
        }
        IbvSendWrList(wr_list)
    }
    pub fn pop(&mut self) -> Option<IbvSendWr>{
        self.0.pop()
    }
    pub fn get(&self, index: usize) -> Option<&IbvSendWr>{
        self.0.get(index)
    }
    pub fn len(&self) -> usize{
        self.0.len()
    }
    pub fn reverse(&mut self) {
        self.0.reverse()
    }
}

pub struct IbvSendWr{
    pub inner: *mut ibv_send_wr,
}
impl IbvSendWr{
    pub fn as_ptr(&self) -> *mut ibv_send_wr{
        self.inner
    }
    pub fn new(
        local_addr: u64,
        lkey: u32,
        remote_addr: u64,
        rkey: u32,
        size: u64,
        offset: u64,
        opcode: IbvWrOpcode,
        signaled: bool,
        wr_id: Option<u64>,
        last_wr_with_imm: bool,
        inline: bool,
    ) -> IbvSendWr {
        let mut local_addr = local_addr + offset;
        let mut remote_addr = remote_addr + offset;
        let mut _wr_id = match wr_id {
            Some(wr_id) => wr_id,
            None => local_addr,
        };
        let length = size;
        let mut last_wr: Option<*mut ibv_send_wr> = None;
        let mut first_wr = None;
        let mut remaining_volume = length;
        let mut first = true;
        if length == 0 {
            let sge = Box::new(ibv_sge{
                addr: local_addr,
                length: 0,
                lkey,
            });
            let sge_ptr = Box::into_raw(sge);
            let mut wr = unsafe { std::mem::zeroed::<ibv_send_wr>() };
            wr.wr_id = _wr_id;
            wr.sg_list = sge_ptr;
            wr.num_sge = 1;
            wr.opcode = opcode.get();
            wr.wr.rdma.remote_addr = remote_addr;
            wr.wr.rdma.rkey = rkey;
            if signaled {
                wr.send_flags = ibv_send_flags::IBV_SEND_SIGNALED.0;
            } else {
                wr.send_flags = 0;
            }
            if inline{
                wr.send_flags |= ibv_send_flags::IBV_SEND_INLINE.0;
            }
            let boxed_wr = Box::new(wr);
            let boxed_wr_ptr = Box::into_raw(boxed_wr);
            first_wr = Some(boxed_wr_ptr);
        } else {
            while remaining_volume > 0 {
                let message = remaining_volume.min(MAX_MESSAGE_SIZE);
                let is_last_message = remaining_volume == message;

                let sge = Box::new(ibv_sge{
                    addr: local_addr,
                    length: message as u32,
                    lkey,
                });
                let sge_ptr = Box::into_raw(sge);
                let mut wr = unsafe { std::mem::zeroed::<ibv_send_wr>() };
                wr.wr_id = _wr_id;
                wr.sg_list = sge_ptr;
                wr.num_sge = 1;
                wr.opcode = opcode.get();
                wr.wr.rdma.remote_addr = remote_addr;
                wr.wr.rdma.rkey = rkey;
                if is_last_message && signaled {
                    wr.send_flags = ibv_send_flags::IBV_SEND_SIGNALED.0;
                } else {
                    wr.send_flags = 0;
                }
                if is_last_message && last_wr_with_imm {
                    wr.opcode = ibv_wr_opcode::IBV_WR_RDMA_WRITE_WITH_IMM;
                    wr.imm_data_invalidated_rkey_union.imm_data = length as u32;
                }

                let boxed_wr = Box::new(wr);
                let boxed_wr_ptr = Box::into_raw(boxed_wr);

                if first {
                    first_wr = Some(boxed_wr_ptr);
                    first = false;
                } else if let Some(last) = last_wr {
                    unsafe {
                        (*last).next = boxed_wr_ptr; // Safely link the previous WR
                    }
                }

                last_wr = Some(boxed_wr_ptr);
                local_addr += message as u64;
                match wr_id{
                    Some(_wr_id) => {},
                    None => {
                        _wr_id += message as u64;
                    }
                }
                remote_addr += message as u64;
                remaining_volume -= message;
            }
        }
        IbvSendWr { 
            inner: first_wr.unwrap(),
        }
    }
}

impl From<*mut ibv_send_wr> for IbvSendWr{
    fn from(ptr: *mut ibv_send_wr) -> Self{
        IbvSendWr{
            inner: ptr,
        }
    }
}

unsafe impl Send for IbvSendWr{}
unsafe impl Sync for IbvSendWr{}

#[derive(Clone)]
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

#[derive(Clone, Debug)]
pub enum IbvWcOpcode{
    Send = 0,
    RdmaWrite = 1,
    RdmaRead = 2,
    CompSwap = 3,
    FetchAdd = 4,
    BindMw = 5,
    LocalInv = 6,
    Tso = 7,
    AtomicWrite = 9,
    Recv = 128,
    RecvRdmaWithImm = 129,
    TmAdd = 130,
    TmDel = 131,
    TmSync = 132,
    TmRecv = 133,
    TmNoTag = 134,
    Driver1 = 135,
    Driver2 = 136,
    Driver3 = 137,
}

impl IbvWcOpcode{
    pub fn as_i32(&self) -> i32{
        self.clone() as i32
    }
}

impl Into <i32> for IbvWcOpcode{
    fn into(self) -> i32{
        self as i32
    }
}

impl From <i32> for IbvWcOpcode{
    fn from(val: i32) -> Self{
        match val{
            0 => IbvWcOpcode::Send,
            1 => IbvWcOpcode::RdmaWrite,
            2 => IbvWcOpcode::RdmaRead,
            3 => IbvWcOpcode::CompSwap,
            4 => IbvWcOpcode::FetchAdd,
            5 => IbvWcOpcode::BindMw,
            6 => IbvWcOpcode::LocalInv,
            7 => IbvWcOpcode::Tso,
            9 => IbvWcOpcode::AtomicWrite,
            128 => IbvWcOpcode::Recv,
            129 => IbvWcOpcode::RecvRdmaWithImm,
            130 => IbvWcOpcode::TmAdd,
            131 => IbvWcOpcode::TmDel,
            132 => IbvWcOpcode::TmSync,
            133 => IbvWcOpcode::TmRecv,
            134 => IbvWcOpcode::TmNoTag,
            135 => IbvWcOpcode::Driver1,
            136 => IbvWcOpcode::Driver2,
            137 => IbvWcOpcode::Driver3,
            _ => IbvWcOpcode::Send,
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
#[repr(C)]
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ControlBufferMetadata{
    pub in_address: u64,
    pub in_rkey: u32,
    pub out_address: u64,
    pub out_rkey: u32,
    pub length: u64,
    pub nreq: u64,
    pub receiver_id: u32,
    pub connection_id: u32,
}

impl ControlBufferMetadata{
    pub const SIZE: usize = std::mem::size_of::<ControlBufferMetadata>();
    pub fn addr(&self) -> *mut u8{
        &self as *const _ as *mut u8
    }
}

pub trait ControlBufferTrait{
    fn length(&self) -> usize;
    fn new() -> Pin<Box<dyn ControlBufferTrait>> where Self: Sized;
    fn size() -> usize where Self: Sized;
    fn address(&self) -> u64;
    fn print(&self);
}

pub struct ControlBuffer{
    in_buffer: InBuffer,
    out_buffer: OutBuffer,
}

pub struct InBuffer{
    local_addr: u64,
    local_rkey: u32,
    local_lkey: u32,
    length: u64,
    buffer: Pin<Box<dyn ControlBufferTrait>>,
    remote_addr: u64,
    remote_rkey: u32,
    mr: Option<IbvMr>,
}

pub struct OutBuffer{
    local_addr: u64,
    local_rkey: u32,
    local_lkey: u32,
    length: u64,
    buffer: Pin<Box<dyn ControlBufferTrait>>,
    remote_addr: u64,
    remote_rkey: u32,
    mr: Option<IbvMr>,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
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

#[derive(Serialize, Deserialize, Debug)]
pub enum SocketCommCommand{
    Mr(ControlBufferMetadata),
    InitQp(u32, Family),
    ConnectQp(QpMetadata),
    Continue,
    Stop,
}



pub fn print_wr_ids(start: *mut ibv_send_wr) -> u64{
    let mut current = start;
    let mut total_len = 0;
    while !unsafe { (*current).next.is_null() } {
        let sge = unsafe { (*current).sg_list };
        
        let sge_len = unsafe { (*sge).length as u64 };
        total_len += sge_len;
        let sge_addr = unsafe { (*sge).addr };
        let remote_addr = unsafe { (*current).wr.rdma.remote_addr };
        let rkey = unsafe { (*current).wr.rdma.rkey };
        let wr_send_flag = unsafe { (*current).send_flags };
        let opcode = unsafe { (*current).opcode };
        let srqn = unsafe { (*current).qp_type.xrc.remote_srqn };
        let sge_ptr_address = sge as u64;
        println!("wr_id: {}, sge_addr: {}, sge_len: {}, remote_addr: {}, rkey {}, send_flag {}, opcode {}, srqn {}, sge_ptr_address {}",
            unsafe { (*current).wr_id }, sge_addr, sge_len, remote_addr, rkey, wr_send_flag, opcode, srqn, sge_ptr_address);
        current = unsafe { (*current).next };

    }
    let sge = &unsafe { (*current).sg_list };
    let sge_len = unsafe { (**sge).length as u64 };
    total_len += sge_len;
    let sge_addr = unsafe { (**sge).addr };
    let remote_addr = unsafe { (*current).wr.rdma.remote_addr };
    let rkey = unsafe { (*current).wr.rdma.rkey };
    let wr_send_flag = unsafe { (*current).send_flags };
    let opcode = unsafe { (*current).opcode };
    let srqn = unsafe { (*current).qp_type.xrc.remote_srqn };
    let sge_ptr_address = sge as *const _ as u64;
    println!("wr_id: {}, sge_addr: {}, sge_len: {}, remote_addr: {}, rkey {}, send_flag {}, opcode {}, srqn {}, sge_ptr_address {}",
        unsafe { (*current).wr_id }, sge_addr, sge_len, remote_addr, rkey, wr_send_flag, opcode, srqn, sge_ptr_address);
    total_len
}

pub fn debug_wr(start: *mut ibv_send_wr) -> WrsDebug{
    let mut wrs_debug = WrsDebug{
        wr_debugs: Vec::new(),
    };
    let mut current = start;
    while !unsafe { (*current).next.is_null() } {
        let sge = unsafe { (*current).sg_list };
        let sge_len = unsafe { (*sge).length as u32 };
        let sge_addr = unsafe { (*sge).addr };
        let remote_addr = unsafe { (*current).wr.rdma.remote_addr };
        let rkey = unsafe { (*current).wr.rdma.rkey };
        let wr_send_flags = unsafe { (*current).send_flags };
        let opcode = unsafe { (*current).opcode };
        let srqn = unsafe { (*current).qp_type.xrc.remote_srqn };
        let wr_debug = WrDebug{
            wr_id: unsafe { (*current).wr_id },
            sge_addr,
            sge_len,
            remote_addr,
            rkey,
            wr_send_flags,
            opcode,
            srqn,
        };
        wrs_debug.wr_debugs.push(wr_debug);
        current = unsafe { (*current).next };
    }
    let sge = &unsafe { (*current).sg_list };
    let sge_len = unsafe { (**sge).length as u32 };
    let sge_addr = unsafe { (**sge).addr };
    let remote_addr = unsafe { (*current).wr.rdma.remote_addr };
    let rkey = unsafe { (*current).wr.rdma.rkey };
    let wr_send_flags = unsafe { (*current).send_flags };
    let opcode = unsafe { (*current).opcode };
    let srqn = unsafe { (*current).qp_type.xrc.remote_srqn };
    let wr_debug = WrDebug{
        wr_id: unsafe { (*current).wr_id },
        sge_addr,
        sge_len,
        remote_addr,
        rkey,
        wr_send_flags,
        opcode,
        srqn,
    };
    wrs_debug.wr_debugs.push(wr_debug);

    wrs_debug
}

#[derive(Debug)]
pub struct WrsDebug{
    wr_debugs: Vec<WrDebug>,
}

#[derive(Debug)]
#[allow(dead_code)]
struct WrDebug{
    wr_id: u64,
    sge_addr: u64,
    sge_len: u32,
    remote_addr: u64,
    rkey: u32,
    wr_send_flags: u32,
    opcode: u32,
    srqn: u32,
}

pub trait Logger {
    fn warn(&self, message: &str);
    fn info(&self, message: &str);
}

#[macro_export]
macro_rules! warn {
    ($logger:expr, $($arg:tt)*) => {{
        rust_warn!($($arg)*);
        $logger.warn(&format!($($arg)*));
    }};
}

#[macro_export]
macro_rules! info {
    ($logger:expr, $($arg:tt)*) => {{
        rust_info!($($arg)*);
        $logger.info(&format!($($arg)*));
    }};
}