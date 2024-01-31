use crate::*;

mod command_service;

pub trait CommandService {
    // Handle the command and return a Response
    fn execute(self, store: &impl Storage) -> CommandResponse;
}


