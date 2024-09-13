# toolchain-arm64.cmake

# 设置目标系统
set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR arm)

set(CROSS_COMPILE /home/wuyufei/cross-toolchains/aarch64/bin/aarch64-linux-gnu-)

# 设置编译器
set(CMAKE_C_COMPILER ${CROSS_COMPILE}gcc)
set(CMAKE_CXX_COMPILER ${CROSS_COMPILE}g++)
set(CMAKE_LINKER ${CROSS_COMPILE}ld)
set(CMAKE_AR ${CROSS_COMPILE}ar)
set(CMAKE_NM ${CROSS_COMPILE}nm)
set(CMAKE_OBJCOPY ${CROSS_COMPILE}objcopy)
set(CMAKE_OBJDUMP ${CROSS_COMPILE}objdump)
set(CMAKE_RANLIB ${CROSS_COMPILE}ranlib)
set(CMAKE_STRIP ${CROSS_COMPILE}strip)

# 设置 sysroot，如果有的话
# set(CMAKE_SYSROOT /path/to/sysroot)

