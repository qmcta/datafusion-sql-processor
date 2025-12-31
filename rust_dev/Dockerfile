FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y build-essential \
    && apt-get install -y curl \
    && apt-get install -y sudo \
    && apt-get -y install locales-all \
    && apt-get -y install libpq-dev \
    && apt-get -y install binutils-mingw-w64-x86-64 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# user追加
ARG USERNAME=debian
ARG GROUPNAME=general
ARG UID=1000
ARG GID=1000
ARG PASSWORD=user
RUN groupadd -g $GID $GROUPNAME || groupmod -n $GROUPNAME $(getent group $GID | cut -d: -f1) && \
    useradd -m -s /bin/bash -u $UID -g $GID -G sudo $USERNAME || usermod -a -G sudo $(getent passwd $UID | cut -d: -f1) && \
    echo $USERNAME:$PASSWORD | chpasswd && \
    echo "$USERNAME   ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Timezone
ENV TZ Asia/Tokyo

# 日本語化
ENV LANG ja_JP.UTF-8
ENV LANGUAGE ja_JP:ja
ENV LC_ALL ja_JP.UTF-8

USER $USERNAME
WORKDIR /work

# rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > /tmp/rustup.sh \
    && chmod +x /tmp/rustup.sh \
    && /tmp/rustup.sh -y --default-toolchain stable \
    && rm /tmp/rustup.sh

ENV PATH="/home/debian/.cargo/bin:${PATH}"

# クロスコンパイル用ターゲットの追加
RUN rustup target add x86_64-pc-windows-gnu

# zig と cargo-zigbuild のインストール
USER root
RUN apt-get update && apt-get install -y xz-utils \
    && curl -L https://ziglang.org/download/0.13.0/zig-linux-x86_64-0.13.0.tar.xz | tar -xJ -C /usr/local \
    && ln -s /usr/local/zig-linux-x86_64-0.13.0/zig /usr/local/bin/zig

USER $USERNAME
RUN cargo install cargo-zigbuild

# ホストとのマウントボリューム上での権限エラーを避けるため、ビルド生成物をコンテナ内のローカル領域に配置します
ENV CARGO_TARGET_DIR="/home/debian/target"

CMD ["bash"]