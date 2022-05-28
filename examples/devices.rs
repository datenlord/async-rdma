use async_rdma::device::DeviceList;

use std::io;

fn main() -> io::Result<()> {
    let dev_list = DeviceList::available()?;
    if dev_list.is_empty() {
        println!("No available rdma devices");
        return Ok(());
    }

    println!("|{:^24}|{:^24}|", "name", "guid");
    println!("|{:-^24}|{:-^24}|", "", "");

    for dev in dev_list.as_slice() {
        let name = dev.name();
        let guid = dev.guid();
        println!("|{name:^24}|{guid:^24x}|");
    }

    Ok(())
}
