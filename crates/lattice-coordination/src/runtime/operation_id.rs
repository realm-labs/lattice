//! Administrative identities are issued from an exact Coordinator inspection.
use super::CoordinatorInspection;
use crate::admin_operation::new_identity;

impl CoordinatorInspection {
    /// Creates one operation identity for this exact run/group/leader revision.
    /// Keep it unchanged when retrying. If it expires, inspect again and make an
    /// explicit new decision instead of silently replacing it during a retry.
    pub fn new_operation_id(&self) -> String {
        new_identity(self.epoch, &self.version)
    }
}
