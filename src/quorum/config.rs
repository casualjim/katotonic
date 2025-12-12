use std::collections::HashSet;

use crate::disco::Member;

#[derive(Debug, Clone)]
pub struct JointQuorum {
  old_names: HashSet<String>,
  old_size: usize,
  joint_size: usize,
}

impl JointQuorum {
  pub fn new(local_config_version: u64, members: &[Member]) -> Self {
    let max_config_version = members
      .iter()
      .map(|m| m.config_version)
      .max()
      .unwrap_or(local_config_version);

    let is_joint = max_config_version > local_config_version;

    let old_names: HashSet<String> = if is_joint {
      members
        .iter()
        .filter(|m| m.config_version <= local_config_version)
        .map(|m| m.name.clone())
        .collect()
    } else {
      members.iter().map(|m| m.name.clone()).collect()
    };

    let old_size = old_names.len();
    let joint_size = members.len();

    Self {
      old_names,
      old_size,
      joint_size,
    }
  }

  pub fn is_joint(&self) -> bool {
    self.old_size != self.joint_size
  }

  pub fn is_old(&self, member_name: &str) -> bool {
    self.old_names.contains(member_name)
  }

  pub fn old_majority(&self) -> usize {
    (self.old_size / 2) + 1
  }

  pub fn joint_majority(&self) -> usize {
    (self.joint_size / 2) + 1
  }
}
