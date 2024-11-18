# mini-lsm-starter

Starter code for Mini-LSM.

## week1 day1
### rust智能指针与Rwlock
`parking_lot::Rwlock` 的`write()`和`read()`会直接返回 `RwlockGuard()`,这几乎可以当作包装的东西来用？
比如RwlockGuard(5), 可以直接打印 出5。

一个智能指针`arc = Arc::new(5)`，在调用 `ref:&i32 = arc.as_ref()`之后，会返回指针指向内容的一个引用 

如果一个结构体 `#[derive(Clone)]`，那么调用 `arc.as_ref().clone()`会进行深拷贝，返回一个 `struct P` 类型
``` rust
use std::sync::Arc;
use parking_lot::RwLock;
fn main() {

    let data_arc= Arc::new(RwLock::new(Arc::new(5)));
    let data_ref = data_arc.as_ref();
    let data_ref_cloned;
    {
        data_ref_cloned = data_arc.read().as_ref().clone();
        println!("data_arc_strong_rc = {}", Arc::strong_count(&data_arc));
    } 
    *data_arc.write() = Arc::new(6);
    println!("data_arc = {}", data_arc.read());
    println!("data_ref = {}", data_ref.read());
    println!("data_ref_cloned = {}", data_ref_cloned);
    
}
```
```
# output
data_arc = 6
data_ref = 6
data_ref_cloned = 5
```