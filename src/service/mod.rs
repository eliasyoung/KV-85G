use std::sync::Arc;
use tracing::debug;
use crate::*;
use crate::command_request::RequestData;

mod command_service;

pub trait CommandService {
    // Handle the command and return a Response
    fn execute(self, store: &impl Storage) -> CommandResponse;
}

// Struct Service
pub struct Service<Store = MemTable> {
    inner: Arc<ServiceInner<Store>>,
}

// Inner Struct of Service
pub struct ServiceInner<Store> {
    store: Store,
}

impl<Store> Clone for Service<Store> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}


impl<Store: Storage> Service<Store> {
    pub fn new(store: Store) -> Self {
        Self {
            inner: Arc::new(ServiceInner {store})
        }
    }

    pub fn execute(&self, cmd:CommandRequest) -> CommandResponse {
        debug!("Got request: {:?}", cmd);
        // TODO: send on_received event
        let res = dispatch(cmd, &self.inner.store);
        debug!("Executed response: {:?}", res);
        // TODO: send on_executed event

        res
    }
}

// Get Response from Request, Handle HGET/HGETALL/HSET for now
pub fn dispatch(cmd: CommandRequest, store: &impl Storage) -> CommandResponse {
    match cmd.request_data {
        Some(RequestData::Hget(hget)) => hget.execute(store),
        Some(RequestData::Hgetall(hget_all)) => hget_all.execute(store),
        Some(RequestData::Hset(hset)) => hset.execute(store),
        None => KvError::InvalidCommand("Request has no data".into()).into(),
        _ => KvError::Internal("Not implemented".into()).into(),
    }
}


#[cfg(test)]
mod tests {
    use std::thread;
    use super::*;
    use crate::{MemTable, Value};

    #[test]
    fn service_should_work() {
        // A Service struct contains Storage is needed
        let service = Service::new(MemTable::default());

        // service is able to running in multi-thread environment, so it's clone should be lightweight.
        let cloned = service.clone();

        // Create a thread, and insert k1, v1 to table {{t1}}
        let handle = thread::spawn(move || {
            let res = cloned.execute(CommandRequest::new_hset("t1", "k1", "v1".into()));
            assert_res_ok(res, &[Value::default()], &[]);
        });
        handle.join().unwrap();

        // In current thread, read value of key {{k1}} in table {{t1}} , it should return {{v1}}
        let res = service.execute(CommandRequest::new_hget("t1", "k1"));
        assert_res_ok(res, &["v1".into()], &[]);
    }
}

#[cfg(test)]
use crate::{Kvpair, Value};

// 测试成功返回的结果
#[cfg(test)]
pub fn assert_res_ok(mut res: CommandResponse, values: &[Value], pairs: &[Kvpair]) {
    res.pairs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(res.status, 200);
    assert_eq!(res.message, "");
    assert_eq!(res.values, values);
    assert_eq!(res.pairs, pairs);
}

// 测试失败返回的结果
#[cfg(test)]
pub fn assert_res_error(res: CommandResponse, code: u32, msg: &str) {
    assert_eq!(res.status, code);
    assert!(res.message.contains(msg));
    assert_eq!(res.values, &[]);
    assert_eq!(res.pairs, &[]);
}

