# Rust语言入门

## 环境配置（wsl）

- 先确保装好了gcc：

```bash
(base) glk@ggg:~/project/my-sql-db$ gcc --version
gcc (Ubuntu 11.4.0-1ubuntu1~22.04) 11.4.0
Copyright (C) 2021 Free Software Foundation, Inc.
This is free software; see the source for copying conditions.  There is NO
warranty; not even for MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
```

- 再下载Rust：

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

- 之后添加环境变量：`~/.cargo/bin`即可
- 验证安装：

```bash
(base) glk@ggg:~/project/my-sql-db$ rustc --version
rustc 1.81.0 (eeb90cda1 2024-09-04)
(base) glk@ggg:~/project/my-sql-db$ cargo --version
cargo 1.81.0 (2dbb1af80 2024-08-20)
```

## 开发配置

- IDE使用[RustRover](https://www.jetbrains.com.cn/rust/?utm_source=baidu&utm_medium=cpc&utm_campaign=CN-BAI-PRO-RustRover-PH-PC&utm_content=Rustrover-ide&utm_term=rust)

## 基础

- [Rust圣经](https://course.rs/basic/intro.html)
- [通过例子学Rust](https://rustwiki.org/zh-CN/rust-by-example/index.html)

## 刷题

- [rustlings](https://rustlings.cool/)

1. `cargo install rustlings`
2. `rustlings init`
3. `cd rustlings`
4. 使用命令`rustlings`开始测评

个人解答仓库：https://github.com/1517005260/rustlings-answer