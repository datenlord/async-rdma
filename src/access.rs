use enumflags2::{bitflags, BitFlags};
use rdma_sys::ibv_access_flags;

/// A wrapper for `ibv_access_flag`, hide the ibv binding types
#[bitflags]
#[repr(u64)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum AccessFlag {
    /// local write permission
    LocalWrite,
    /// remote write permission
    RemoteWrite,
    /// remote read permission
    RemoteRead,
    /// remote atomic operation permission
    RemoteAtomic,
    /// enable memory window binding
    MwBind,
    /// use byte offset from beginning of MR to access this MR, instead of a pointer address
    ZeroBased,
    /// create an on-demand paging MR
    OnDemand,
    /// huge pages are guaranteed to be used for this MR, only used with `OnDemand`
    HugeTlb,
    /// allow system to reorder accesses to the MR to improve performance
    RelaxOrder,
}

/// Convert `BitFlags<AccessFlag>` into `ibv_access_flags`
#[inline]
#[must_use]
pub(crate) fn flags_into_ibv_access(flags: BitFlags<AccessFlag>) -> ibv_access_flags {
    let mut ret = ibv_access_flags(0);
    if flags.contains(AccessFlag::LocalWrite) {
        ret |= ibv_access_flags::IBV_ACCESS_LOCAL_WRITE;
    }
    if flags.contains(AccessFlag::RemoteWrite) {
        ret |= ibv_access_flags::IBV_ACCESS_REMOTE_WRITE;
    }
    if flags.contains(AccessFlag::RemoteRead) {
        ret |= ibv_access_flags::IBV_ACCESS_REMOTE_READ;
    }
    if flags.contains(AccessFlag::RemoteAtomic) {
        ret |= ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC;
    }
    if flags.contains(AccessFlag::MwBind) {
        ret |= ibv_access_flags::IBV_ACCESS_MW_BIND;
    }
    if flags.contains(AccessFlag::ZeroBased) {
        ret |= ibv_access_flags::IBV_ACCESS_ZERO_BASED;
    }
    if flags.contains(AccessFlag::OnDemand) {
        ret |= ibv_access_flags::IBV_ACCESS_ON_DEMAND;
    }
    if flags.contains(AccessFlag::HugeTlb) {
        ret |= ibv_access_flags::IBV_ACCESS_HUGETLB;
    }
    if flags.contains(AccessFlag::RelaxOrder) {
        ret |= ibv_access_flags::IBV_ACCESS_RELAXED_ORDERING;
    }
    ret
}

/// Convert `ibv_access_flags` into `BitFlags<AccessFlag>`
#[inline]
#[must_use]
pub(crate) fn ibv_access_into_flags(access: ibv_access_flags) -> BitFlags<AccessFlag> {
    let mut ret = BitFlags::<AccessFlag>::empty();
    if (access & ibv_access_flags::IBV_ACCESS_LOCAL_WRITE).0 != 0 {
        ret |= AccessFlag::LocalWrite;
    }
    if (access & ibv_access_flags::IBV_ACCESS_LOCAL_WRITE).0 != 0 {
        ret |= AccessFlag::LocalWrite;
    }
    if (access & ibv_access_flags::IBV_ACCESS_REMOTE_READ).0 != 0 {
        ret |= AccessFlag::RemoteRead;
    }
    if (access & ibv_access_flags::IBV_ACCESS_REMOTE_WRITE).0 != 0 {
        ret |= AccessFlag::RemoteWrite;
    }
    if (access & ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC).0 != 0 {
        ret |= AccessFlag::RemoteAtomic;
    }
    if (access & ibv_access_flags::IBV_ACCESS_MW_BIND).0 != 0 {
        ret |= AccessFlag::MwBind;
    }
    if (access & ibv_access_flags::IBV_ACCESS_ZERO_BASED).0 != 0 {
        ret |= AccessFlag::ZeroBased;
    }
    if (access & ibv_access_flags::IBV_ACCESS_ON_DEMAND).0 != 0 {
        ret |= AccessFlag::OnDemand;
    }
    if (access & ibv_access_flags::IBV_ACCESS_HUGETLB).0 != 0 {
        ret |= AccessFlag::HugeTlb;
    }
    if (access & ibv_access_flags::IBV_ACCESS_RELAXED_ORDERING).0 != 0 {
        ret |= AccessFlag::RelaxOrder;
    }
    ret
}

#[cfg(test)]
mod access_test {
    use super::*;
    use crate::{memory_region::IbvAccess, RdmaBuilder};
    use std::alloc::Layout;

    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn flags_into_ibv_access_test() {
        let rdma = RdmaBuilder::default().build().unwrap();
        let layout = Layout::new::<[u8; 4096]>();
        let access = AccessFlag::LocalWrite | AccessFlag::RemoteRead;
        let mr = rdma.alloc_local_mr_with_access(layout, access).unwrap();
        assert_eq!(mr.ibv_access(), flags_into_ibv_access(access));
    }

    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn ibv_access_into_flags_test() {
        let rdma = RdmaBuilder::default().build().unwrap();
        let layout = Layout::new::<[u8; 4096]>();
        let access = AccessFlag::LocalWrite | AccessFlag::RemoteRead;
        let mr = rdma.alloc_local_mr_with_access(layout, access).unwrap();
        assert_eq!(access, ibv_access_into_flags(mr.ibv_access()));
    }
}
