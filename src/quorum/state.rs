use std::{
  fs::{self, File, OpenOptions},
  io::{self, Write as _},
  path::{Path, PathBuf},
  time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

use crate::{cbor, Result};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct QuorumState {
  pub current_term: u64,
  pub voted_term: u64,
  pub high_watermark: u64,
  pub config_version: u64,
}

impl QuorumState {
  pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
    let path = path.as_ref();
    match fs::read(path) {
      Ok(bytes) => {
        let state: Self = cbor::from_slice(&bytes)?;
        state.validate()?;
        Ok(state)
      }
      Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(Self::default()),
      Err(err) => Err(err.into()),
    }
  }

  pub fn persist<P: AsRef<Path>>(&self, path: P) -> Result<()> {
    self.validate()?;
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
      fs::create_dir_all(parent)?;
    }

    let bytes = cbor::to_vec(self)?;
    let tmp_path = temp_path_for(path);

    let mut file = OpenOptions::new()
      .create(true)
      .write(true)
      .truncate(true)
      .open(&tmp_path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);

    replace_file(&tmp_path, path)?;

    if let Some(parent) = path.parent()
      && let Ok(dir) = File::open(parent) {
        let _ = dir.sync_all();
      }

    Ok(())
  }

  pub fn bump_term(&mut self) -> u64 {
    let new_term = self.current_term.saturating_add(1);
    self.current_term = new_term;
    if self.voted_term < new_term {
      self.voted_term = new_term;
    }
    self.high_watermark = 0;
    new_term
  }

  pub fn set_current_term(&mut self, term: u64) -> Result<()> {
    if term < self.current_term {
      return Err(io::Error::new(io::ErrorKind::InvalidInput, "current_term regression").into());
    }

    if term != self.current_term {
      self.current_term = term;
      if self.voted_term < term {
        self.voted_term = term;
      }
      self.high_watermark = 0;
    }

    Ok(())
  }

  pub fn set_voted_term(&mut self, term: u64) -> Result<bool> {
    if term < self.current_term {
      return Err(
        io::Error::new(
          io::ErrorKind::InvalidInput,
          "voted_term must be >= current_term",
        )
        .into(),
      );
    }
    if term < self.voted_term {
      return Err(io::Error::new(io::ErrorKind::InvalidInput, "voted_term regression").into());
    }
    if term > self.voted_term {
      self.voted_term = term;
      return Ok(true);
    }
    Ok(false)
  }

  pub fn advance_high_watermark(&mut self, new_hw: u64) -> Result<bool> {
    if new_hw < self.high_watermark {
      return Err(io::Error::new(io::ErrorKind::InvalidInput, "high_watermark regression").into());
    }
    if new_hw > self.high_watermark {
      self.high_watermark = new_hw;
      return Ok(true);
    }
    Ok(false)
  }

  pub fn set_config_version(&mut self, new_version: u64) -> Result<bool> {
    if new_version < self.config_version {
      return Err(io::Error::new(io::ErrorKind::InvalidInput, "config_version regression").into());
    }
    if new_version > self.config_version {
      self.config_version = new_version;
      return Ok(true);
    }
    Ok(false)
  }

  fn validate(&self) -> Result<()> {
    if self.voted_term < self.current_term {
      return Err(io::Error::new(io::ErrorKind::InvalidData, "voted_term < current_term").into());
    }
    Ok(())
  }
}

fn temp_path_for(path: &Path) -> PathBuf {
  let file_name = path
    .file_name()
    .and_then(|n| n.to_str())
    .unwrap_or("katotonic.state");
  let nanos = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap_or_default()
    .as_nanos();
  let tmp_name = format!("{file_name}.tmp-{}-{nanos}", std::process::id());
  path.with_file_name(tmp_name)
}

fn replace_file(tmp_path: &Path, path: &Path) -> io::Result<()> {
  match fs::rename(tmp_path, path) {
    Ok(()) => Ok(()),
    Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
      fs::remove_file(path)?;
      fs::rename(tmp_path, path)
    }
    Err(err) => Err(err),
  }
}

#[cfg(test)]
mod tests {
  use std::{
    env, fs,
    time::{SystemTime, UNIX_EPOCH},
  };

  use proptest::prelude::*;

  use super::QuorumState;

  fn temp_state_path(label: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap_or_default()
      .as_nanos();
    let mut dir = env::temp_dir();
    dir.push(format!(
      "katotonic-test-{label}-{}-{nanos}",
      std::process::id()
    ));
    fs::create_dir_all(&dir).unwrap();
    dir.join("state.cbor")
  }

  #[test]
  fn test_persist_and_load_round_trip() {
    let path = temp_state_path("roundtrip");
    let dir = path.parent().unwrap().to_path_buf();

    let state = QuorumState {
      current_term: 5,
      voted_term: 6,
      high_watermark: 42,
      config_version: 2,
    };
    state.persist(&path).unwrap();

    let loaded = QuorumState::load(&path).unwrap();
    assert_eq!(state, loaded);

    fs::remove_dir_all(dir).unwrap();
  }

  #[test]
  fn test_persist_overwrites_safely() {
    let path = temp_state_path("overwrite");
    let dir = path.parent().unwrap().to_path_buf();

    let state1 = QuorumState {
      current_term: 1,
      voted_term: 1,
      high_watermark: 10,
      config_version: 0,
    };
    state1.persist(&path).unwrap();

    let state2 = QuorumState {
      current_term: 2,
      voted_term: 2,
      high_watermark: 20,
      config_version: 1,
    };
    state2.persist(&path).unwrap();

    let loaded = QuorumState::load(&path).unwrap();
    assert_eq!(state2, loaded);

    fs::remove_dir_all(dir).unwrap();
  }

  #[test]
  fn test_load_rejects_invalid_voted_term() {
    let path = temp_state_path("invalid-vote");
    let dir = path.parent().unwrap().to_path_buf();

    let invalid = QuorumState {
      current_term: 10,
      voted_term: 9,
      high_watermark: 0,
      config_version: 0,
    };
    let bytes = crate::cbor::to_vec(&invalid).unwrap();
    fs::write(&path, bytes).unwrap();

    assert!(QuorumState::load(&path).is_err());

    fs::remove_dir_all(dir).unwrap();
  }

  #[test]
  fn test_helpers_prevent_regressions() {
    let mut state = QuorumState::default();
    state.set_current_term(5).unwrap();
    assert!(state.set_current_term(4).is_err());

    state.set_voted_term(6).unwrap();
    assert!(state.set_voted_term(5).is_err());

    state.advance_high_watermark(100).unwrap();
    assert!(state.advance_high_watermark(99).is_err());

    state.set_config_version(2).unwrap();
    assert!(state.set_config_version(1).is_err());
  }

  #[derive(Debug, Clone)]
  enum Op {
    SetCurrent(u64),
    SetVoted(u64),
    AdvanceHw(u64),
    SetConfig(u64),
    BumpTerm,
  }

  fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
      (0u64..200).prop_map(Op::SetCurrent),
      (0u64..200).prop_map(Op::SetVoted),
      (0u64..5000).prop_map(Op::AdvanceHw),
      (0u64..50).prop_map(Op::SetConfig),
      Just(Op::BumpTerm),
    ]
  }

  proptest! {
    #[test]
    fn prop_state_invariants_hold(ops in proptest::collection::vec(op_strategy(), 0..200)) {
      let mut state = QuorumState::default();

      for op in ops {
        let before = state.clone();
        let _ = match op {
          Op::SetCurrent(term) => state.set_current_term(term),
          Op::SetVoted(term) => state.set_voted_term(term).map(|_| ()),
          Op::AdvanceHw(hw) => state.advance_high_watermark(hw).map(|_| ()),
          Op::SetConfig(v) => state.set_config_version(v).map(|_| ()),
          Op::BumpTerm => { state.bump_term(); Ok(()) }
        };

        prop_assert!(state.current_term >= before.current_term);
        prop_assert!(state.voted_term >= before.voted_term);
        prop_assert!(state.voted_term >= state.current_term);
        prop_assert!(state.config_version >= before.config_version);
        if state.current_term == before.current_term {
          prop_assert!(state.high_watermark >= before.high_watermark);
        }
      }
    }
  }
}
