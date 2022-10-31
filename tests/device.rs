mod imm_flag {
    use async_rdma::RdmaBuilder;

    #[test]
    #[should_panic = "IBV_WC_WITH_IMM is initialized or being initialized but has not yet completed"]
    fn imm_flag_double_set() {
        let _rdma = RdmaBuilder::default().set_imm_flag_in_wc(2).unwrap();
        let _new_rdma = RdmaBuilder::default().set_imm_flag_in_wc(3).unwrap();
    }
}
