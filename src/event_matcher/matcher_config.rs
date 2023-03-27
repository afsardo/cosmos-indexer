use serde::{Deserialize, Serialize};
use std::fs::File;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MatcherConfig {
    pub events: Vec<MatcherEvent>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MatcherEvent {
    pub name: String,
    pub key: String,
    pub patterns: Vec<Pattern>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Pattern {
    pub key: String,
    pub value: String,
}

pub fn load_matcher_config() -> MatcherConfig {
    let config_file = File::open("config.yaml").unwrap();
    let matcher_config = serde_yaml::from_reader::<File, MatcherConfig>(config_file).unwrap();
    matcher_config
}
