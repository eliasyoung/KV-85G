use crate::*;

impl CommandService for Hget {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        // match store.get(&self.table, &self.key) {
        //     Ok(Some(v)) => v.into(),
        //     Ok(None) => KvError::NotFound(self.table, self.key).into(),
        //     Err(e) => e.into(),
        // }
        todo!()
    }
}

impl CommandService for Hgetall {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        // match store.get_all(&self.table) {
        //     Ok(v) => v.into(),
        //     Err(e) => e.into(),
        // }
        todo!()
    }
}

impl CommandService for Hset {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        todo!()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::command_request::RequestData;

    #[test]
    fn hset_should_work() {
        todo!()
    }

    #[test]
    fn hget_should_work() {
        todo!()
    }

    #[test]
    fn hget_with_non_exist_key_should_return_404() {
        todo!()
    }

    #[test]
    fn hget_all_should_work() {
        todo!()
    }

    // Get Response from Request, could handle HGET/HGETALL/HSET for now.
    fn dispatch(cmd: CommandRequest, store: &impl Storage) -> CommandResponse {
        // match cmd.request_data.unwrap() {
        //
        // }
        todo!()
    }

    // Test where result will be success.
    fn assert_res_ok(mut res: CommandResponse, values: &[Value], pairs: &[Kvpair]) {
        todo!()
    }

    // Test where result will be error.
    fn assert_res_error(mut res: CommandResponse, code: u32, msg: &str) {
        todo!()
    }
}