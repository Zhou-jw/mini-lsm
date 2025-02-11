# mini-lsm-starter

Starter code for Mini-LSM.

## week1 day0 配置rust
### 支持cargo doc --open

```
//安装 xdg-open
sudo apt-get install -y xdg-utils
// 安装wslu 
https://github.com/wslutilities/wslu?tab=readme-ov-file
```

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

## week1 day2
### self-referencing
```rust
// https://docs.rs/ouroboros/latest/ouroboros/attr.self_referencing.html

//example
use ouroboros::self_referencing;

#[self_referencing]
struct MyStruct {
    int_data: i32,
    float_data: f32,
    #[borrows(int_data)]
    // the 'this lifetime is created by the #[self_referencing] macro
    // and should be used on all references marked by the #[borrows] macro
    int_reference: &'this i32,
    #[borrows(mut float_data)]
    float_reference: &'this mut f32,
}

fn main() {
    // The builder is created by the #[self_referencing] macro
    // and is used to create the struct
    let mut my_value = MyStructBuilder {
        int_data: 42,
        float_data: 3.14,

        // Note that the name of the field in the builder
        // is the name of the field in the struct + `_builder`
        // ie: {field_name}_builder
        // the closure that assigns the value for the field will be passed
        // a reference to the field(s) defined in the #[borrows] macro

        int_reference_builder: |int_data: &i32| int_data,
        float_reference_builder: |float_data: &mut f32| float_data,
    }.build();

    // The fields in the original struct can not be accessed directly
    // The builder creates accessor methods which are called borrow_{field_name}()

    // Prints 42
    println!("{:?}", my_value.borrow_int_data());
    // Prints 3.14
    println!("{:?}", my_value.borrow_float_reference());
    // Sets the value of float_data to 84.0
    my_value.with_mut(|fields| {
        **fields.float_reference = (**fields.int_reference as f32) * 2.0;
    });

    // We can hold on to this reference...
    let int_ref = *my_value.borrow_int_reference();
    println!("{:?}", *int_ref);
    // As long as the struct is still alive.
    drop(my_value);
    // This will cause an error!
    // println!("{:?}", *int_ref);
}
```

### 可派生trait
```
https://rustwiki.org/zh-CN/book/appendix-03-derivable-traits.html
```

## week3 day6
### Task 3: Engine Interface and Serializable Validation
1. serializable validation
go through committed txn in range (read_ts, expected_commit_ts)

if read_set overlaps with write_set of those txns, validation fails

if validation succeeds, commit the write_batch, insert write_set into `self.inner.mvcc().committed_txns`



