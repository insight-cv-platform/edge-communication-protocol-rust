FROM python:3.10-buster AS deps

RUN curl https://sh.rustup.rs -sSf | bash -s -- -y
RUN echo 'source $HOME/.cargo/env' >> $HOME/.bashrc

WORKDIR /opt
RUN /usr/local/bin/python -m pip install --upgrade pip
RUN /usr/local/bin/python -m pip install --upgrade maturin~=0.12


FROM deps AS builder

COPY . /opt
WORKDIR /opt

RUN echo cargo -v test | bash -l -s
RUN echo maturin build -r | bash -l -s

FROM python:3.10-buster

WORKDIR /opt

COPY API /opt/protocol/API
COPY --from=builder /opt/target/wheels /opt/wheels

RUN /usr/local/bin/python -m pip install --upgrade pip
RUN pip install wheels/*.whl
