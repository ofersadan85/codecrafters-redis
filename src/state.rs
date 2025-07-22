use std::{collections::HashMap, sync::Arc};
use tokio::sync::{Mutex, Notify};

use crate::resp::RespData;

#[derive(Debug, Default)]
pub struct WaitingList {
    pub count: u32,
    pub signal: Arc<Notify>,
}

#[derive(Debug, Default)]
pub struct AppState {
    pub kv: HashMap<String, RespData>,
    pub waiting_lists: HashMap<String, WaitingList>,
}
pub type State = Arc<Mutex<AppState>>;

impl AppState {
    pub fn prune_waiting_lists(&mut self) {
        self.waiting_lists.retain(|_, list| list.count > 0);
    }
}
